import { execFile } from "node:child_process";
import { cpus, freemem, totalmem } from "node:os";
import type { MachineMetrics, StorageDisk } from "@openbot/contracts/ipc";
import { Effect } from "effect";
import { readLinuxMemory } from "./server-memory";

const VM_STAT_TIMEOUT_MS = 5_000;
/** A CPU reading needs two samples. A first read waits this long for the second one. */
const CPU_FIRST_SAMPLE_MS = 1_000;

interface CpuTimes {
  idle: number;
  total: number;
  at: number;
}

interface MemoryReading {
  availableBytes: number;
  totalBytes: number;
}

/**
 * Reads the disk, memory and CPU use of this machine for Server settings and the machine alerts.
 * Memory counts what programs can still use, as the system monitor of each platform does, so the
 * file cache does not read as used. CPU is the use of all cores since the reading before.
 */
export class MachineMetricsSampler {
  readonly #disk: () => Effect.Effect<StorageDisk>;
  readonly #platform: NodeJS.Platform;
  #previous: CpuTimes | null = null;

  constructor(disk: () => Effect.Effect<StorageDisk>, platform: NodeJS.Platform = process.platform) {
    this.#disk = disk;
    this.#platform = platform;
  }

  readonly read = Effect.fn("MachineMetrics.read")(function* (this: MachineMetricsSampler) {
    const [disk, memory, cpuPercent] = yield* Effect.all([this.#disk(), readMemory(this.#platform), this.#cpu()], {
      concurrency: "unbounded",
    });
    return {
      diskFreeBytes: disk.freeBytes,
      diskTotalBytes: disk.totalBytes,
      memoryAvailableBytes: memory?.availableBytes ?? null,
      memoryTotalBytes: memory?.totalBytes ?? null,
      cpuPercent,
      cpuCores: cpus().length,
      checkedAt: new Date().toISOString(),
    } satisfies MachineMetrics;
  });

  #cpu = Effect.fn("MachineMetrics.cpu")(function* (this: MachineMetricsSampler) {
    let previous = this.#previous;
    if (!previous || Date.now() - previous.at < CPU_FIRST_SAMPLE_MS) {
      previous = cpuTimes();
      yield* Effect.sleep(CPU_FIRST_SAMPLE_MS);
    }
    const current = cpuTimes();
    this.#previous = current;
    const total = current.total - previous.total;
    if (total <= 0) return null;
    const busy = 1 - (current.idle - previous.idle) / total;
    return Math.round(Math.min(1, Math.max(0, busy)) * 100);
  });
}

function cpuTimes(): CpuTimes {
  let idle = 0;
  let total = 0;
  for (const { times } of cpus()) {
    idle += times.idle;
    total += times.user + times.nice + times.sys + times.idle + times.irq;
  }
  return { idle, total, at: Date.now() };
}

function readMemory(platform: NodeJS.Platform): Effect.Effect<MemoryReading | null> {
  if (platform === "linux")
    return readLinuxMemory().pipe(
      Effect.map(({ sample }) => ({ availableBytes: sample.available, totalBytes: sample.total })),
      Effect.catch(() => Effect.succeed(null)),
    );
  if (platform === "darwin")
    return readVmStat.pipe(
      Effect.map((text) => darwinMemory(text, totalmem())),
      Effect.catch(() => Effect.succeed(null)),
    );
  // Windows reports the memory that programs can still use as free.
  return Effect.sync(() => ({ availableBytes: freemem(), totalBytes: totalmem() }));
}

const readVmStat = Effect.callback<string, Error>((resume) => {
  const child = execFile("vm_stat", [], { encoding: "utf8", timeout: VM_STAT_TIMEOUT_MS }, (error, stdout) => {
    resume(error ? Effect.fail(error) : Effect.succeed(stdout));
  });
  return Effect.sync(() => {
    child.kill();
  });
});

/**
 * Used memory as Activity Monitor counts it: app memory (anonymous pages that cannot be purged),
 * wired memory, and the compressor. The rest, with the file cache, is available. Null when the
 * output has an unknown shape.
 */
export function darwinMemory(text: string, totalBytes: number): MemoryReading | null {
  const pageSize = Number(/page size of (\d+) bytes/u.exec(text)?.[1]);
  const pages = (label: string): number => Number(new RegExp(`^${label}:\\s+(\\d+)\\.`, "mu").exec(text)?.[1]);
  const used =
    (pages("Anonymous pages") -
      pages("Pages purgeable") +
      pages("Pages wired down") +
      pages("Pages occupied by compressor")) *
    pageSize;
  if (!Number.isFinite(used) || used < 0 || totalBytes <= 0) return null;
  return { availableBytes: Math.max(0, Math.round(totalBytes - used)), totalBytes };
}
