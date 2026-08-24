import { describe, expect, it, vi } from "bun:test";
import { type ChildProcess, spawn } from "node:child_process";
import { EventEmitter } from "node:events";
import * as fs from "node:fs/promises";
import * as os from "node:os";
import * as path from "node:path";
import { nativeProcessBindings } from "@gajae-code/utils/native-process";
import { Broker } from "../src/sdk/broker/broker";
import {
	brokerDiscoveryPath,
	brokerProcessIncarnation,
	readBrokerDiscovery,
	writeBrokerDiscovery,
} from "../src/sdk/broker/discovery";
import {
	brokerOwnerForTest,
	canRetireStaleBrokerForTest,
	ensureBroker,
	isLegacyUnstampedDiscoveryForTest,
	matchesExpectedBrokerAuthorityForTest,
	registerBrokerOwnerForTest,
	signalExactBrokerForTest,
} from "../src/sdk/broker/ensure";
import {
	resolveSdkInternalSpawnCommandForTest,
	resolveSdkPackageAuthority,
	resolveSdkPackageGeneration,
} from "../src/sdk/broker/runtime";
import { BrokerTransport, brokerShutdownSendAction } from "../src/sdk/broker/transport";
import { SdkClient } from "../src/sdk/client/client";

const temp = () => fs.mkdtemp(path.join(os.tmpdir(), "gjc-broker-generation-"));

async function nextFrame(ws: WebSocket): Promise<Record<string, unknown>> {
	return await new Promise((resolve, reject) => {
		const timeout = setTimeout(() => reject(new Error("Timed out waiting for broker frame")), 2_000);
		ws.addEventListener(
			"message",
			event => {
				clearTimeout(timeout);
				resolve(JSON.parse(String(event.data)) as Record<string, unknown>);
			},
			{ once: true },
		);
		ws.addEventListener(
			"error",
			() => {
				clearTimeout(timeout);
				reject(new Error("Broker WebSocket error"));
			},
			{ once: true },
		);
		ws.addEventListener(
			"close",
			event => {
				clearTimeout(timeout);
				reject(new Error(`Broker WebSocket closed (${event.code})`));
			},
			{ once: true },
		);
	});
}

async function connectTransport(url: string, token: string): Promise<WebSocket> {
	const ws = new WebSocket(`${url}/?token=${token}`);
	await new Promise<void>((resolve, reject) => {
		ws.addEventListener("open", () => resolve(), { once: true });
		ws.addEventListener("error", () => reject(new Error("Broker WebSocket error")), { once: true });
	});
	return ws;
}

async function waitForCondition(predicate: () => boolean, label: string): Promise<void> {
	const deadline = Date.now() + 2_000;
	while (!predicate()) {
		if (Date.now() >= deadline) throw new Error(`Timed out waiting for ${label}`);
		await Bun.sleep(10);
	}
}

function ownedTestChild(): ChildProcess & { signals: NodeJS.Signals[] } {
	const signals: NodeJS.Signals[] = [];
	return Object.assign(new EventEmitter(), {
		pid: process.pid,
		exitCode: null as number | null,
		signalCode: null as NodeJS.Signals | null,
		signals,
		kill(signal: NodeJS.Signals): boolean {
			signals.push(signal);
			return true;
		},
	}) as unknown as ChildProcess & { signals: NodeJS.Signals[] };
}
function standInBrokerProcess(): { pid: number; incarnation: string; kill: () => void } {
	const child = spawn(process.execPath, ["-e", "await Bun.sleep(3_600_000)"], { stdio: "ignore" });
	if (child.pid === undefined) throw new Error("stand-in broker pid unavailable");
	const incarnation = brokerProcessIncarnation(child.pid);
	if (!incarnation) throw new Error("stand-in broker incarnation unavailable");
	return {
		pid: child.pid,
		incarnation,
		kill() {
			child.kill("SIGKILL");
		},
	};
}

function olderPackageVersion(version: string): string {
	const match = /^(\d+)\.(\d+)\.(\d+)$/.exec(version);
	if (!match) throw new Error(`Test requires a stable package version, got ${version}`);
	const major = Number(match[1]);
	const minor = Number(match[2]);
	const patch = Number(match[3]);
	if (patch > 0) return `${major}.${minor}.${patch - 1}`;
	if (minor > 0) return `${major}.${minor - 1}.0`;
	if (major > 0) return `${major - 1}.0.0`;
	throw new Error("Test cannot derive an older package version from 0.0.0");
}

function staleBroker(agentDir: string): Broker {
	const authority = resolveSdkPackageAuthority();
	return new Broker({
		agentDir,
		packageGeneration: "stale-gen",
		packageVersion: olderPackageVersion(authority.packageVersion),
		installationIdentity: authority.installationIdentity,
	});
}

async function cleanup(dir: string, broker?: Broker): Promise<void> {
	await broker?.stop().catch(() => {});
	await brokerOwnerForTest(dir)
		?.stop()
		.catch(() => {});
	await fs.rm(dir, { recursive: true, force: true });
}

describe("sdk broker package generation", () => {
	it("is stable for the current package tree and shaped like a digest", () => {
		const first = resolveSdkPackageGeneration();
		expect(first).toMatch(/^[0-9a-f]{64}$/);
		expect(resolveSdkPackageGeneration()).toBe(first);
	});

	it("rejects same-generation startup winners with mismatched authority", () => {
		const authority = resolveSdkPackageAuthority();
		const discovery = {
			version: 1,
			protocolVersion: 3,
			packageGeneration: authority.generation,
			packageVersion: authority.packageVersion,
			installationIdentity: authority.installationIdentity,
			ownerId: "startup-race-winner",
			pid: process.pid,
			incarnation: brokerProcessIncarnation(process.pid)!,
			host: "127.0.0.1",
			port: 1,
			url: "ws://127.0.0.1:1",
			token: "startup-race-token",
			startedAt: Date.now(),
			heartbeatAt: Date.now(),
		} as const;
		expect(
			matchesExpectedBrokerAuthorityForTest(
				{ ...discovery, packageVersion: "0.0.1" },
				authority.generation,
				authority.packageVersion,
				authority.installationIdentity,
			),
		).toBe(false);
		expect(
			matchesExpectedBrokerAuthorityForTest(
				{ ...discovery, installationIdentity: `${authority.installationIdentity}-foreign` },
				authority.generation,
				authority.packageVersion,
				authority.installationIdentity,
			),
		).toBe(false);
		expect(
			matchesExpectedBrokerAuthorityForTest(
				discovery,
				authority.generation,
				authority.packageVersion,
				authority.installationIdentity,
			),
		).toBe(true);
	});

	it("binds compiled generation to content, not only size and mtime", async () => {
		const dir = await temp();
		const executable = path.join(dir, "gjc-copy");
		try {
			await fs.copyFile(process.execPath, executable);
			const markerName = "internal-source-marker-2178-abcd.txt";
			const evidence = {
				execPath: executable,
				markerPath: `/$bunfs/root/${markerName}`,
				embeddedFiles: [{ name: markerName }],
			} as const;
			const before = resolveSdkInternalSpawnCommandForTest("broker-internal", evidence);
			const stat = await fs.stat(executable);
			const bytes = await fs.readFile(executable);
			bytes[0] ^= 0xff;
			await fs.writeFile(executable, bytes);
			await fs.utimes(executable, stat.atime, stat.mtime);
			const after = resolveSdkInternalSpawnCommandForTest("broker-internal", evidence);
			expect(after.generation).not.toBe(before.generation);
		} finally {
			await fs.rm(dir, { recursive: true, force: true });
		}
	});

	it("reuses a live broker whose generation matches the caller's expectation", async () => {
		const dir = await temp();
		const broker = new Broker({ agentDir: dir, packageGeneration: "current-gen" });
		try {
			const published = await broker.start();
			const discovery = await ensureBroker({ agentDir: dir, expectedPackageGeneration: "current-gen" });
			expect(discovery.pid).toBe(published.pid);
			expect(discovery.packageGeneration).toBe("current-gen");
		} finally {
			await cleanup(dir, broker);
		}
	}, 15_000);

	it("uses the current package generation by default", async () => {
		const dir = await temp();
		const broker = new Broker({ agentDir: dir, packageGeneration: resolveSdkPackageGeneration() });
		try {
			const published = await broker.start();
			const discovery = await ensureBroker({ agentDir: dir });
			expect(discovery.pid).toBe(published.pid);
		} finally {
			await cleanup(dir, broker);
		}
	}, 15_000);

	it("retires a stale-generation broker and replaces it with a current one", async () => {
		const dir = await temp();
		const stale = staleBroker(dir);
		try {
			const published = await stale.start();
			const authority = resolveSdkPackageAuthority();
			const expected = authority.generation;
			expect(expected).not.toBe("stale-gen");
			const discovery = await ensureBroker({ agentDir: dir, expectedPackageGeneration: expected });
			// The replacement is a freshly spawned broker publishing the generation
			// of the package tree this process would spawn.
			expect(discovery.pid).not.toBe(published.pid);
			expect(discovery.packageGeneration).toBe(expected);
			expect(discovery.packageVersion).toBe(authority.packageVersion);
			expect(discovery.installationIdentity).toBe(authority.installationIdentity);
			// The stale broker no longer owns discovery.
			const current = await readBrokerDiscovery(dir);
			expect(current?.pid).toBe(discovery.pid);
			expect(current?.incarnation).toBe(discovery.incarnation);
			const immediate = await ensureBroker({ agentDir: dir, expectedPackageGeneration: expected });
			expect(immediate.pid).toBe(discovery.pid);
			expect(immediate.incarnation).toBe(discovery.incarnation);
		} finally {
			await cleanup(dir, stale);
		}
	}, 30_000);

	it("does not reuse a stale broker when retirement cannot be proven", async () => {
		const dir = await temp();
		const stale = staleBroker(dir);
		const stop = vi.spyOn(stale, "stop").mockResolvedValue(undefined);
		try {
			const published = await stale.start();
			const expected = resolveSdkPackageGeneration();
			await expect(ensureBroker({ agentDir: dir, expectedPackageGeneration: expected })).rejects.toThrow(
				"stale broker retirement was not verified",
			);
			const current = await readBrokerDiscovery(dir);
			expect(current?.pid).toBe(published.pid);
		} finally {
			stop.mockRestore();
			await cleanup(dir, stale);
		}
	}, 15_000);

	it("does not satisfy a concurrent caller with a different generation", async () => {
		const dir = await temp();
		const stale = staleBroker(dir);
		try {
			await stale.start();
			const expected = resolveSdkPackageGeneration();
			const first = ensureBroker({ agentDir: dir, expectedPackageGeneration: expected });
			const second = ensureBroker({ agentDir: dir, expectedPackageGeneration: "different-generation" });
			expect((await first).packageGeneration).toBe(expected);
			await expect(second).rejects.toThrow("does not match expected generation different-generation");
		} finally {
			await cleanup(dir, stale);
		}
	}, 30_000);

	it("does not retire a newer local owner for an obsolete caller generation", async () => {
		const dir = await temp();
		try {
			const current = await ensureBroker({ agentDir: dir });
			const obsolete = `${current.packageGeneration}-obsolete-caller`;
			await expect(ensureBroker({ agentDir: dir, expectedPackageGeneration: obsolete })).rejects.toThrow(
				"changed before local owner retirement",
			);
			const retained = await readBrokerDiscovery(dir);
			expect(retained?.pid).toBe(current.pid);
			expect(retained?.incarnation).toBe(current.incarnation);
		} finally {
			await cleanup(dir);
		}
	}, 30_000);

	it("does not signal the ensuring process for an unreachable self-PID discovery", async () => {
		const dir = await temp();
		try {
			const incarnation = brokerProcessIncarnation(process.pid);
			expect(incarnation).toBeString();
			await writeBrokerDiscovery(dir, {
				version: 1,
				protocolVersion: 3,
				packageGeneration: "stale-gen",
				ownerId: "self-pid-test",
				pid: process.pid,
				incarnation: incarnation!,
				host: "127.0.0.1",
				port: 1,
				url: "ws://127.0.0.1:1",
				token: "unreachable",
				startedAt: Date.now(),
				heartbeatAt: Date.now(),
			});
			await expect(
				ensureBroker({ agentDir: dir, expectedPackageGeneration: resolveSdkPackageGeneration() }),
			).rejects.toThrow("stale broker retirement was not verified");
		} finally {
			await fs.rm(dir, { recursive: true, force: true });
		}
	}, 15_000);

	it("fails closed without signalling a legacy discovery with opaque generation drift", async () => {
		const dir = await temp();
		const authority = resolveSdkPackageAuthority();
		const pid = process.ppid;
		const processRef = nativeProcessBindings().Process.fromPid(pid);
		const incarnation = processRef?.incarnation ?? brokerProcessIncarnation(pid);
		const signalRoot = vi.fn(() => true);
		const fromPid = vi.spyOn(nativeProcessBindings().Process, "fromPid").mockReturnValue({
			incarnation,
			signalRoot,
		} as never);
		try {
			expect(incarnation).toBeString();
			await writeBrokerDiscovery(dir, {
				version: 1,
				protocolVersion: 3,
				packageGeneration: "legacy-opaque-generation",
				ownerId: "legacy-discovery",
				pid,
				incarnation: incarnation!,
				host: "127.0.0.1",
				port: 1,
				url: "ws://127.0.0.1:1",
				token: "legacy-token",
				startedAt: Date.now(),
				heartbeatAt: Date.now(),
			});
			await expect(ensureBroker({ agentDir: dir, expectedPackageGeneration: authority.generation })).rejects.toThrow(
				`stale broker retirement was not verified. Stop the broker at pid ${pid} before deleting ${brokerDiscoveryPath(dir)}.`,
			);
			expect(signalRoot).not.toHaveBeenCalled();
		} finally {
			fromPid.mockRestore();
			await fs.rm(dir, { recursive: true, force: true });
		}
	}, 15_000);

	it("retires a live unstamped unknown-generation broker through authenticated shutdown", async () => {
		const dir = await temp();
		const token = "unstamped-unknown-generation-token";
		const standIn = standInBrokerProcess();
		const stopped = Promise.withResolvers<void>();
		const stop = vi.fn(async () => {
			await fs.rm(path.join(dir, "sdk", "broker.json"), { force: true });
			standIn.kill();
			stopped.resolve();
		});
		const transport = new BrokerTransport(
			{
				handleRequest: vi.fn(async () => ({ ok: true, result: { drained: true } })),
				stop,
			} as unknown as Broker,
			token,
		);
		const port = await transport.start();
		try {
			await writeBrokerDiscovery(dir, {
				version: 1,
				protocolVersion: 3,
				packageGeneration: "unknown",
				ownerId: "legacy-unstamped",
				pid: standIn.pid,
				incarnation: standIn.incarnation,
				host: "127.0.0.1",
				port,
				url: `ws://127.0.0.1:${port}`,
				token,
				startedAt: Date.now(),
				heartbeatAt: Date.now(),
			});
			const authority = resolveSdkPackageAuthority();
			const replacement = await ensureBroker({
				agentDir: dir,
				expectedPackageGeneration: authority.generation,
			});
			await stopped.promise;
			expect(stop).toHaveBeenCalledTimes(1);
			expect(replacement.pid).not.toBe(standIn.pid);
			expect(replacement.packageGeneration).toBe(authority.generation);
			expect(replacement.packageVersion).toBe(authority.packageVersion);
			expect(replacement.installationIdentity).toBe(authority.installationIdentity);
		} finally {
			standIn.kill();
			await transport.stop();
			await cleanup(dir);
		}
	}, 30_000);

	it("preserves admitted work on an unstamped broker instead of signalling", async () => {
		const dir = await temp();
		const token = "unstamped-admitted-work-token";
		const admitted = Promise.withResolvers<{ ok: true; result: { drained: true } }>();
		const admittedStarted = Promise.withResolvers<void>();
		const stopped = Promise.withResolvers<void>();
		const stop = vi.fn(async () => {
			await fs.rm(path.join(dir, "sdk", "broker.json"), { force: true });
			stopped.resolve();
		});
		const handleRequest = vi.fn(() => {
			admittedStarted.resolve();
			return admitted.promise;
		});
		const transport = new BrokerTransport({ handleRequest, stop } as unknown as Broker, token);
		const port = await transport.start();
		const incarnation = brokerProcessIncarnation(process.pid);
		const client = await SdkClient.connect(`ws://127.0.0.1:${port}`, token, { reconnectAttempts: 0 });
		try {
			expect(incarnation).toBeString();
			await writeBrokerDiscovery(dir, {
				version: 1,
				protocolVersion: 3,
				packageGeneration: "unknown",
				ownerId: "legacy-unstamped-admitted",
				pid: process.pid,
				incarnation: incarnation!,
				host: "127.0.0.1",
				port,
				url: `ws://127.0.0.1:${port}`,
				token,
				startedAt: Date.now(),
				heartbeatAt: Date.now(),
			});
			const work = client.global("session.list", {});
			await admittedStarted.promise;
			await expect(
				ensureBroker({ agentDir: dir, expectedPackageGeneration: resolveSdkPackageGeneration() }),
			).rejects.toThrow("stale broker retirement was not verified");
			expect(stop).not.toHaveBeenCalled();
			admitted.resolve({ ok: true, result: { drained: true } });
			await expect(work).resolves.toMatchObject({ ok: true, result: { drained: true } });
			await stopped.promise;
			expect(stop).toHaveBeenCalledTimes(1);
		} finally {
			await client.close().catch(() => {});
			await transport.stop();
			await cleanup(dir);
		}
	}, 15_000);

	it("does not spawn over a live unstamped pid after heartbeat TTL when shutdown does not answer", async () => {
		const dir = await temp();
		const incarnation = brokerProcessIncarnation(process.pid);
		const connect = vi.spyOn(SdkClient, "connect").mockRejectedValue(new Error("broker unreachable"));
		try {
			expect(incarnation).toBeString();
			await writeBrokerDiscovery(dir, {
				version: 1,
				protocolVersion: 3,
				packageGeneration: "unknown",
				ownerId: "legacy-unstamped-ttl",
				pid: process.pid,
				incarnation: incarnation!,
				host: "127.0.0.1",
				port: 1,
				url: "ws://127.0.0.1:1",
				token: "legacy-unstamped-ttl-token",
				startedAt: Date.now(),
				heartbeatAt: Date.now(),
			});
			const authority = resolveSdkPackageAuthority();
			await expect(
				ensureBroker({
					agentDir: dir,
					expectedPackageGeneration: authority.generation,
					heartbeatTtlMs: 500,
				}),
			).rejects.toThrow(
				`stale broker retirement was not verified. Stop the broker at pid ${process.pid} before deleting ${brokerDiscoveryPath(dir)}.`,
			);
			expect(await readBrokerDiscovery(dir, 500)).toBeNull();
			const retained = JSON.parse(await fs.readFile(brokerDiscoveryPath(dir), "utf8")) as { pid: number };
			expect(retained.pid).toBe(process.pid);
		} finally {
			connect.mockRestore();
			await cleanup(dir);
		}
	}, 15_000);
	it("does not spawn over an unstamped live pid whose heartbeat already expired", async () => {
		const dir = await temp();
		const incarnation = brokerProcessIncarnation(process.pid);
		const connect = vi.spyOn(SdkClient, "connect").mockRejectedValue(new Error("broker unreachable"));
		try {
			expect(incarnation).toBeString();
			await writeBrokerDiscovery(dir, {
				version: 1,
				protocolVersion: 3,
				packageGeneration: "unknown",
				ownerId: "legacy-unstamped-expired",
				pid: process.pid,
				incarnation: incarnation!,
				host: "127.0.0.1",
				port: 1,
				url: "ws://127.0.0.1:1",
				token: "legacy-unstamped-expired-token",
				startedAt: Date.now() - 60_000,
				heartbeatAt: Date.now() - 60_000,
			});
			const authority = resolveSdkPackageAuthority();
			await expect(
				ensureBroker({
					agentDir: dir,
					expectedPackageGeneration: authority.generation,
				}),
			).rejects.toThrow(
				`stale broker retirement was not verified. Stop the broker at pid ${process.pid} before deleting ${brokerDiscoveryPath(dir)}.`,
			);
			expect(connect).toHaveBeenCalled();
		} finally {
			connect.mockRestore();
			await cleanup(dir);
		}
	}, 15_000);

	it("does not signal a reused pid published by an unstamped discovery", async () => {
		const dir = await temp();
		const pid = process.ppid;
		const processRef = nativeProcessBindings().Process.fromPid(pid);
		const liveIncarnation = processRef?.incarnation ?? brokerProcessIncarnation(pid);
		const originalFromPid = nativeProcessBindings().Process.fromPid.bind(nativeProcessBindings().Process);
		const signalRoot = vi.fn(() => true);
		const fromPid = vi.spyOn(nativeProcessBindings().Process, "fromPid").mockImplementation(candidate => {
			const real = originalFromPid(candidate);
			if (!real) return real;
			if (candidate === pid) {
				return { incarnation: real.incarnation, signalRoot } as never;
			}
			return real;
		});
		try {
			expect(liveIncarnation).toBeString();
			await writeBrokerDiscovery(dir, {
				version: 1,
				protocolVersion: 3,
				packageGeneration: "unknown",
				ownerId: "legacy-pid-reuse",
				pid,
				incarnation: `${liveIncarnation}-reused`,
				host: "127.0.0.1",
				port: 1,
				url: "ws://127.0.0.1:1",
				token: "legacy-pid-reuse-token",
				startedAt: Date.now(),
				heartbeatAt: Date.now(),
			});
			const authority = resolveSdkPackageAuthority();
			await expect(
				ensureBroker({
					agentDir: dir,
					expectedPackageGeneration: authority.generation,
				}),
			).rejects.toThrow(
				`stale broker retirement was not verified. Published pid ${pid} is live with a different incarnation; do not signal it. Delete ${brokerDiscoveryPath(dir)} only after confirming it is not the SDK broker.`,
			);
			expect(signalRoot).not.toHaveBeenCalled();
		} finally {
			fromPid.mockRestore();
			await cleanup(dir);
		}
	}, 30_000);

	it("retires the issue-shaped null-identity discovery record through shutdown", async () => {
		const dir = await temp();
		const token = "issue-4910-null-identity-token";
		const standIn = standInBrokerProcess();
		const stopped = Promise.withResolvers<void>();
		const stop = vi.fn(async () => {
			await fs.rm(path.join(dir, "sdk", "broker.json"), { force: true });
			standIn.kill();
			stopped.resolve();
		});
		const transport = new BrokerTransport(
			{
				handleRequest: vi.fn(async () => ({ ok: true, result: { drained: true } })),
				stop,
			} as unknown as Broker,
			token,
		);
		const port = await transport.start();
		try {
			await fs.mkdir(path.join(dir, "sdk"), { recursive: true });
			await Bun.write(
				brokerDiscoveryPath(dir),
				`${JSON.stringify({
					version: 1,
					protocolVersion: 3,
					packageGeneration: "unknown",
					packageVersion: null,
					installationIdentity: null,
					ownerId: "issue-4910",
					pid: standIn.pid,
					incarnation: standIn.incarnation,
					host: "127.0.0.1",
					port,
					url: `ws://127.0.0.1:${port}`,
					token,
					startedAt: Date.now(),
					heartbeatAt: Date.now(),
				})}\n`,
			);
			const authority = resolveSdkPackageAuthority();
			const replacement = await ensureBroker({
				agentDir: dir,
				expectedPackageGeneration: authority.generation,
			});
			await stopped.promise;
			expect(stop).toHaveBeenCalledTimes(1);
			expect(replacement.packageGeneration).toBe(authority.generation);
			expect(replacement.pid).not.toBe(standIn.pid);
		} finally {
			standIn.kill();
			await transport.stop();
			await cleanup(dir);
		}
	}, 30_000);

	it("fails closed when Darwin cannot signal an identity-bound stale broker", () => {
		const originalPlatform = process.platform;
		const processRef = {
			incarnation: "darwin:1700000000:123456",
			signalRoot: vi.fn(() => false),
		};
		const fromPid = vi.spyOn(nativeProcessBindings().Process, "fromPid").mockReturnValue(processRef as never);
		Object.defineProperty(process, "platform", { configurable: true, value: "darwin" });
		try {
			expect(signalExactBrokerForTest(4_242, processRef.incarnation)).toBe(false);
			expect(processRef.signalRoot).toHaveBeenCalledWith(os.constants.signals.SIGTERM);
		} finally {
			Object.defineProperty(process, "platform", { configurable: true, value: originalPlatform });
			fromPid.mockRestore();
		}
	});

	it("retires only a strictly older broker from the same installation", () => {
		const authority = resolveSdkPackageAuthority();
		const base = {
			version: 1 as const,
			protocolVersion: 3 as const,
			packageGeneration: "older-generation",
			packageVersion: olderPackageVersion(authority.packageVersion),
			installationIdentity: authority.installationIdentity,
			ownerId: "authority-test",
			pid: 1,
			incarnation: "linux:1",
			host: "127.0.0.1" as const,
			port: 1,
			url: "ws://127.0.0.1:1",
			token: "authority-test-token",
			startedAt: Date.now(),
			heartbeatAt: Date.now(),
		};

		expect(canRetireStaleBrokerForTest(base, authority)).toBe(true);
		expect(canRetireStaleBrokerForTest({ ...base, packageVersion: authority.packageVersion }, authority)).toBe(false);
		expect(
			canRetireStaleBrokerForTest(
				{ ...base, packageVersion: `${Number(authority.packageVersion.split(".")[0]) + 1}.0.0` },
				authority,
			),
		).toBe(false);
		expect(
			canRetireStaleBrokerForTest(
				{ ...base, installationIdentity: `${authority.installationIdentity}-other` },
				authority,
			),
		).toBe(false);
		const betaAuthority = { ...authority, packageVersion: "1.0.0-beta.10" };
		expect(canRetireStaleBrokerForTest({ ...base, packageVersion: "1.0.0-beta.2" }, betaAuthority)).toBe(true);
		expect(
			canRetireStaleBrokerForTest(
				{ ...base, packageVersion: "1.0.0-beta.10" },
				{ ...authority, packageVersion: "1.0.0" },
			),
		).toBe(true);
		expect(
			canRetireStaleBrokerForTest(
				{ ...base, packageVersion: "1.0.0" },
				{ ...authority, packageVersion: "1.0.0-beta.10" },
			),
		).toBe(false);
		expect(canRetireStaleBrokerForTest({ ...base, packageVersion: undefined }, authority)).toBe(false);
		expect(
			canRetireStaleBrokerForTest(
				{ ...base, installationIdentity: undefined, packageGeneration: "unknown" },
				authority,
			),
		).toBe(false);
		expect(isLegacyUnstampedDiscoveryForTest({ ...base, packageVersion: undefined })).toBe(false);
		expect(isLegacyUnstampedDiscoveryForTest({ ...base, installationIdentity: undefined })).toBe(false);
		expect(
			isLegacyUnstampedDiscoveryForTest({
				...base,
				packageGeneration: "unknown",
				packageVersion: undefined,
				installationIdentity: undefined,
			}),
		).toBe(true);
		expect(isLegacyUnstampedDiscoveryForTest(base)).toBe(false);
	});

	it("does not signal a newer broker when the caller generation is stale", async () => {
		const dir = await temp();
		const authority = resolveSdkPackageAuthority();
		const signalRoot = vi.fn(() => true);
		const fromPid = vi
			.spyOn(nativeProcessBindings().Process, "fromPid")
			.mockReturnValue({ incarnation: brokerProcessIncarnation(process.pid), signalRoot } as never);
		try {
			const incarnation = brokerProcessIncarnation(process.pid);
			expect(incarnation).toBeString();
			await writeBrokerDiscovery(dir, {
				version: 1,
				protocolVersion: 3,
				packageGeneration: "newer-broker-generation",
				packageVersion: `${Number(authority.packageVersion.split(".")[0]) + 1}.0.0`,
				installationIdentity: authority.installationIdentity,
				ownerId: "newer-broker",
				pid: process.pid,
				incarnation: incarnation!,
				host: "127.0.0.1",
				port: 1,
				url: "ws://127.0.0.1:1",
				token: "newer-broker-token",
				startedAt: Date.now(),
				heartbeatAt: Date.now(),
			});
			await expect(
				ensureBroker({ agentDir: dir, expectedPackageGeneration: "older-caller-generation" }),
			).rejects.toThrow("changed before retirement");
			expect(signalRoot).not.toHaveBeenCalled();
		} finally {
			fromPid.mockRestore();
			await fs.rm(dir, { recursive: true, force: true });
		}
	});

	it("does not connect with a token to an unvalidated stale endpoint", async () => {
		const dir = await temp();
		const authority = resolveSdkPackageAuthority();
		const incarnation = brokerProcessIncarnation(process.pid);
		const connect = vi.spyOn(SdkClient, "connect");
		try {
			expect(incarnation).toBeString();
			await writeBrokerDiscovery(dir, {
				version: 1,
				protocolVersion: 3,
				packageGeneration: "legacy-endpoint-generation",
				packageVersion: olderPackageVersion(authority.packageVersion),
				installationIdentity: authority.installationIdentity,
				ownerId: "unvalidated-endpoint",
				pid: process.pid,
				incarnation: incarnation!,
				host: "127.0.0.1",
				port: 1,
				url: "ws://localhost:1/unvalidated",
				token: "must-not-be-sent",
				startedAt: Date.now(),
				heartbeatAt: Date.now(),
			});
			await expect(ensureBroker({ agentDir: dir })).rejects.toThrow("stale broker retirement was not verified");
			expect(connect).not.toHaveBeenCalled();
		} finally {
			connect.mockRestore();
			await fs.rm(dir, { recursive: true, force: true });
		}
	}, 15_000);

	it("refuses to signal a substituted process incarnation", () => {
		const processRef = {
			incarnation: "darwin:1700000000:999999",
			signalRoot: vi.fn(() => true),
		};
		const fromPid = vi.spyOn(nativeProcessBindings().Process, "fromPid").mockReturnValue(processRef as never);
		try {
			expect(signalExactBrokerForTest(4_242, "darwin:1700000000:123456")).toBe(false);
			expect(processRef.signalRoot).not.toHaveBeenCalled();
		} finally {
			fromPid.mockRestore();
		}
	});

	it("waits for admitted broker work before completing authenticated shutdown", async () => {
		const admitted = Promise.withResolvers<{ ok: true; result: { drained: true } }>();
		const stopped = Promise.withResolvers<void>();
		const handleRequest = vi.fn(() => admitted.promise);
		const stop = vi.fn(async () => {
			stopped.resolve();
		});
		const transport = new BrokerTransport({ handleRequest, stop } as unknown as Broker, "generation-transport-token");
		const port = await transport.start();
		const ws = await connectTransport(`ws://127.0.0.1:${port}`, "generation-transport-token");
		try {
			expect(await nextFrame(ws)).toEqual({ type: "broker_hello", protocolVersion: 3 });
			ws.send(JSON.stringify({ type: "broker_request", id: "admitted", operation: "session.list", input: {} }));
			await waitForCondition(() => handleRequest.mock.calls.length === 1, "admitted broker request");
			expect(handleRequest).toHaveBeenCalledTimes(1);

			const shutdownResponse = nextFrame(ws);
			ws.send(JSON.stringify({ type: "broker_request", id: "shutdown", operation: "broker.shutdown", input: {} }));
			expect(await shutdownResponse).toEqual({
				type: "broker_response",
				id: "shutdown",
				ok: true,
				result: { accepted: true },
			});
			const lateResponse = nextFrame(ws);
			ws.send(JSON.stringify({ type: "broker_request", id: "late", operation: "session.list", input: {} }));
			expect(await lateResponse).toEqual({
				type: "broker_response",
				ok: false,
				error: { code: "unavailable", message: "broker is shutting down" },
			});
			await Bun.sleep(25);
			expect(stop).not.toHaveBeenCalled();

			const workResponse = nextFrame(ws);
			admitted.resolve({ ok: true, result: { drained: true } });
			expect(await workResponse).toEqual({
				type: "broker_response",
				id: "admitted",
				ok: true,
				result: { drained: true },
			});
			await stopped.promise;
			expect(stop).toHaveBeenCalledTimes(1);
		} finally {
			ws.close();
			await transport.stop();
		}
	});

	it("stops after a dropped shutdown acknowledgement instead of retaining a stale broker", () => {
		expect(brokerShutdownSendAction(0)).toBe("close");
	});

	it("does not reap a ready local owner when its publication is missing", async () => {
		const dir = await temp();
		const authority = resolveSdkPackageAuthority();
		const child = ownedTestChild();
		const owner = registerBrokerOwnerForTest(dir, child);
		const incarnation = brokerProcessIncarnation(process.pid);
		try {
			expect(incarnation).toBeString();
			const discovery = {
				version: 1 as const,
				protocolVersion: 3 as const,
				packageGeneration: authority.generation,
				packageVersion: authority.packageVersion,
				installationIdentity: authority.installationIdentity,
				ownerId: "ready-owner-missing-publication",
				pid: process.pid,
				incarnation: incarnation!,
				host: "127.0.0.1" as const,
				port: 1,
				url: "ws://127.0.0.1:1",
				token: "ready-owner-missing-publication-token",
				startedAt: Date.now(),
				heartbeatAt: Date.now(),
			};
			await writeBrokerDiscovery(dir, discovery);
			expect(owner.markReady(discovery)).toBe(true);
			await fs.rm(path.join(dir, "sdk", "broker.json"), { force: true });
			await expect(
				ensureBroker({
					agentDir: dir,
					expectedPackageGeneration: authority.generation,
					expectedPackageVersion: authority.packageVersion,
					expectedInstallationIdentity: authority.installationIdentity,
				}),
			).rejects.toThrow("refusing destructive cleanup of a ready broker");
			expect(child.signals).toEqual([]);
			expect(brokerOwnerForTest(dir)).toBe(owner);
		} finally {
			(child as unknown as { exitCode: number | null }).exitCode = 0;
			child.emit("exit", 0, null);
			await owner.stop().catch(() => {});
			await fs.rm(dir, { recursive: true, force: true });
		}
	});

	it("does not force-kill an equal-version exact local owner while work is admitted", async () => {
		const dir = await temp();
		const authority = resolveSdkPackageAuthority();
		const token = "owned-retirement-timeout-token";
		const child = ownedTestChild();
		const admitted = Promise.withResolvers<{ ok: true; result: { drained: true } }>();
		const admittedStarted = Promise.withResolvers<void>();
		const stopped = Promise.withResolvers<void>();
		const handleRequest = vi.fn(() => {
			admittedStarted.resolve();
			return admitted.promise;
		});
		const transport = new BrokerTransport(
			{
				handleRequest,
				stop: vi.fn(async () => {
					await fs.rm(path.join(dir, "sdk", "broker.json"), { force: true });
					(child as unknown as { exitCode: number | null }).exitCode = 0;
					child.emit("exit", 0, null);
					stopped.resolve();
				}),
			} as unknown as Broker,
			token,
		);
		const port = await transport.start();
		const owner = registerBrokerOwnerForTest(dir, child);
		const incarnation = brokerProcessIncarnation(process.pid);
		const client = await SdkClient.connect(`ws://127.0.0.1:${port}`, token, { reconnectAttempts: 0 });
		try {
			expect(incarnation).toBeString();
			await writeBrokerDiscovery(dir, {
				version: 1,
				protocolVersion: 3,
				packageGeneration: "same-version-source-generation",
				packageVersion: authority.packageVersion,
				installationIdentity: authority.installationIdentity,
				ownerId: "owned-retirement-timeout",
				pid: process.pid,
				incarnation: incarnation!,
				host: "127.0.0.1",
				port,
				url: `ws://127.0.0.1:${port}`,
				token,
				startedAt: Date.now(),
				heartbeatAt: Date.now(),
			});
			const work = client.global("session.list", {});
			await admittedStarted.promise;
			await expect(
				ensureBroker({
					agentDir: dir,
					expectedPackageGeneration: authority.generation,
					expectedPackageVersion: authority.packageVersion,
					expectedInstallationIdentity: authority.installationIdentity,
				}),
			).rejects.toThrow("stale broker retirement was not verified");
			expect(child.signals).toEqual([]);
			expect(brokerOwnerForTest(dir)).toBe(owner);

			admitted.resolve({ ok: true, result: { drained: true } });
			await expect(work).resolves.toMatchObject({ ok: true, result: { drained: true } });
			await stopped.promise;
		} finally {
			await client.close().catch(() => {});
			await transport.stop();
			await brokerOwnerForTest(dir)
				?.stop()
				.catch(() => {});
			await fs.rm(dir, { recursive: true, force: true });
		}
	}, 15_000);

	it("replaces a verified drained equal-version local owner exactly once", async () => {
		const dir = await temp();
		const authority = resolveSdkPackageAuthority();
		const token = "owned-retirement-drained-token";
		const child = ownedTestChild();
		const stopped = Promise.withResolvers<void>();
		const stop = vi.fn(async () => {
			await fs.rm(path.join(dir, "sdk", "broker.json"), { force: true });
			(child as unknown as { exitCode: number | null }).exitCode = 0;
			child.emit("exit", 0, null);
			stopped.resolve();
		});
		const transport = new BrokerTransport(
			{
				handleRequest: vi.fn(async () => ({ ok: true, result: { drained: true } })),
				stop,
			} as unknown as Broker,
			token,
		);
		const port = await transport.start();
		registerBrokerOwnerForTest(dir, child);
		const incarnation = brokerProcessIncarnation(process.pid);
		try {
			expect(incarnation).toBeString();
			await writeBrokerDiscovery(dir, {
				version: 1,
				protocolVersion: 3,
				packageGeneration: "same-version-source-generation",
				packageVersion: authority.packageVersion,
				installationIdentity: authority.installationIdentity,
				ownerId: "owned-retirement-drained",
				pid: process.pid,
				incarnation: incarnation!,
				host: "127.0.0.1",
				port,
				url: `ws://127.0.0.1:${port}`,
				token,
				startedAt: Date.now(),
				heartbeatAt: Date.now(),
			});
			const replacement = await ensureBroker({
				agentDir: dir,
				expectedPackageGeneration: authority.generation,
				expectedPackageVersion: authority.packageVersion,
				expectedInstallationIdentity: authority.installationIdentity,
			});
			expect(await stopped.promise).toBeUndefined();
			expect(stop).toHaveBeenCalledTimes(1);
			expect(replacement.pid).not.toBe(process.pid);
			const reused = await ensureBroker({
				agentDir: dir,
				expectedPackageGeneration: authority.generation,
				expectedPackageVersion: authority.packageVersion,
				expectedInstallationIdentity: authority.installationIdentity,
			});
			expect(reused.pid).toBe(replacement.pid);
			expect(reused.incarnation).toBe(replacement.incarnation);
			expect(stop).toHaveBeenCalledTimes(1);
		} finally {
			await transport.stop();
			await cleanup(dir);
		}
	}, 30_000);

	it("serializes concurrent stale-broker retirements into one replacement", async () => {
		const dir = await temp();
		const stale = staleBroker(dir);
		try {
			const published = await stale.start();
			const expected = resolveSdkPackageGeneration();
			const [first, second] = await Promise.all([
				ensureBroker({ agentDir: dir, expectedPackageGeneration: expected }),
				ensureBroker({ agentDir: dir, expectedPackageGeneration: expected }),
			]);
			expect(first.pid).toBe(second.pid);
			expect(first.pid).not.toBe(published.pid);
		} finally {
			await cleanup(dir, stale);
		}
	}, 30_000);
});
