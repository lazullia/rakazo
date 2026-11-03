import type { ChildProcessWithoutNullStreams } from "node:child_process";
import { spawn, spawnSync } from "node:child_process";
import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import type { PiStageSqlConnection, PiStageSqlPool } from "./pi-stage-postgres.js";

const DOCKER = "docker";
const clients = new Set<ChildProcessWithoutNullStreams>();
interface Descriptor {
  id: string;
  name: string;
  image: string;
  labelValue: string;
}

/** Never reads DATABASE_URL or contacts an existing app database. */
export function testDatabaseDescriptor(): Descriptor {
  const file = process.env.PI_STAGE_TEST_DESCRIPTOR;
  if (!file) throw new Error("Explicit isolated test descriptor required");
  const descriptor = JSON.parse(readFileSync(file, "utf8")) as Descriptor;
  if (
    !/^pi-stage-offline-[a-f0-9]{12}$/.test(descriptor.name) ||
    !/^[a-f0-9]{64}$/.test(descriptor.id) ||
    !/^sha256:[a-f0-9]{64}$/.test(descriptor.image) ||
    descriptor.name !== `pi-stage-offline-${descriptor.labelValue}`
  )
    throw new Error("Invalid test DB descriptor");
  const inspect = spawnSync(DOCKER, ["inspect", descriptor.id], { encoding: "utf8" });
  if (inspect.status !== 0) throw new Error("Owned test DB unavailable");
  const info = JSON.parse(inspect.stdout)[0];
  if (
    info.Id !== descriptor.id ||
    info.Name !== `/${descriptor.name}` ||
    info.Image !== descriptor.image ||
    info.Config.Labels?.["codex.pi-stage-offline"] !== descriptor.labelValue ||
    !info.State.Running ||
    info.HostConfig.NetworkMode !== "none" ||
    info.HostConfig.Privileged ||
    Object.keys(info.HostConfig.PortBindings ?? {}).length ||
    info.Mounts.some((mount: { Type: string }) => mount.Type !== "tmpfs")
  )
    throw new Error("Test DB isolation check failed");
  return descriptor;
}

function literal(value: unknown) {
  if (value === null) return "NULL";
  if (typeof value === "string") return `'${value.replaceAll("'", "''")}'`;
  if (typeof value === "number" && Number.isFinite(value)) return String(value);
  if (typeof value === "boolean") return value ? "TRUE" : "FALSE";
  throw new Error("Unsupported test SQL parameter");
}

/**
 * A test-only psql session over docker exec stdin/stdout. It keeps a real backend
 * transaction open across calls, with no network, host port or production URL.
 * Production uses pg's parameter binding; this adapter quotes only fake test values.
 */
class PsqlConnection implements PiStageSqlConnection {
  private readonly child: ChildProcessWithoutNullStreams;
  private buffer = "";
  private pending?: {
    marker: string;
    lines: string[];
    resolve: (value: { rows: Array<Record<string, unknown>> }) => void;
    reject: (error: Error) => void;
    timer: NodeJS.Timeout;
  };
  private closed = false;
  constructor(descriptor: Descriptor, admin: boolean) {
    this.child = spawn(
      DOCKER,
      [
        "exec",
        "-i",
        descriptor.id,
        "psql",
        "-X",
        "-qAt",
        "-v",
        "ON_ERROR_STOP=1",
        "-U",
        admin ? "pi_test" : "pi_stage_app",
        "-d",
        "pi_stage_test",
      ],
      { stdio: "pipe" },
    );
    clients.add(this.child);
    this.child.stdout.setEncoding("utf8");
    this.child.stdout.on("data", (data: string) => {
      this.buffer += data;
      while (this.buffer.includes("\n")) {
        const newline = this.buffer.indexOf("\n");
        const line = this.buffer.slice(0, newline).trim();
        this.buffer = this.buffer.slice(newline + 1);
        const pending = this.pending;
        if (!pending) continue;
        if (line === pending.marker) {
          clearTimeout(pending.timer);
          this.pending = undefined;
          try {
            pending.resolve({ rows: pending.lines.filter(Boolean).map((row) => JSON.parse(row)) });
          } catch {
            pending.reject(new Error("Invalid isolated psql result"));
          }
        } else if (line) pending.lines.push(line);
      }
    });
    // Errors can contain fake SQL; never forward connection bodies to product code.
    this.child.stderr.resume();
    const failed = () => {
      this.closed = true;
      clients.delete(this.child);
      if (this.pending) {
        clearTimeout(this.pending.timer);
        this.pending.reject(new Error("Isolated SQL session ended"));
        this.pending = undefined;
      }
    };
    this.child.on("exit", failed);
    this.child.on("error", failed);
    this.child.stdin.on("error", failed);
  }
  query(sql: string, values: unknown[] = []): Promise<{ rows: Array<Record<string, unknown>> }> {
    if (this.closed || this.pending)
      return Promise.reject(new Error("Invalid isolated SQL session state"));
    const expanded = sql
      .replace(/\$(\d+)/g, (_, index: string) => literal(values[Number(index) - 1]))
      .trim()
      .replace(/;$/, "");
    const statement = /^SELECT\b/i.test(expanded)
      ? `SELECT row_to_json(result) FROM (${expanded}) AS result`
      : expanded;
    const marker = `pi_done_${randomUUID().replaceAll("-", "")}`;
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.child.kill();
        reject(new Error("Isolated SQL query timed out"));
      }, 10_000);
      this.pending = { marker, lines: [], resolve, reject, timer };
      this.child.stdin.write(`${statement};\n\\echo ${marker}\n`);
    });
  }
  release(error?: Error) {
    if (error) this.child.kill();
    else if (!this.closed) this.child.stdin.end();
  }
}

export class IsolatedPsqlPool implements PiStageSqlPool {
  constructor(private readonly admin = false) {}
  async connect() {
    return new PsqlConnection(testDatabaseDescriptor(), this.admin);
  }
}

export async function closeTestSqlSessions() {
  await Promise.all(
    [...clients].map(
      (child) =>
        new Promise<void>((resolve) => {
          child.once("exit", () => resolve());
          child.stdin.end();
        }),
    ),
  );
}
