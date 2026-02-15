import { beforeEach, describe, expect, it, vi } from "vitest";

import { services } from "../services/index.js";
import { serviceContainer } from "../services/ServiceContainer.js";
import * as executor from "../subagent/executor.js";
import { getAgentNames, getSubagent } from "../subagent/get-agents.js";
import { streamChatResponse } from "../stream/streamChatResponse.js";

import { subagentTool } from "./subagent.js";

vi.mock("../subagent/get-agents.js");
vi.mock("../services/ServiceContainer.js", () => ({
  serviceContainer: {
    get: vi.fn(),
    set: vi.fn(),
  },
}));
vi.mock("../services/index.js", () => ({
  services: {
    toolPermissions: {
      getState: vi.fn().mockReturnValue({ currentMode: "default" }),
    },
    systemMessage: {
      getSystemMessage: vi.fn().mockResolvedValue(""),
    },
    chatHistory: {
      getSessionId: vi.fn().mockReturnValue("parent-session-id"),
      addToolResult: vi.fn(),
      isReady: vi.fn().mockReturnValue(true),
    },
  },
}));
vi.mock("../stream/streamChatResponse.js", () => ({
  streamChatResponse: vi.fn(),
}));
vi.mock("../util/cli.js", () => ({
  escapeEvents: {
    on: vi.fn(),
    removeListener: vi.fn(),
  },
}));
vi.mock("../util/logger.js", () => ({
  logger: {
    debug: vi.fn(),
    error: vi.fn(),
  },
}));

describe("subagentTool", () => {
  const modelServiceState = {} as any;

  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(serviceContainer.get).mockResolvedValue(modelServiceState);
  });

  it("preprocess throws when agent is not found", async () => {
    vi.mocked(getAgentNames).mockReturnValue(["code-agent"]);

    const tool = await subagentTool();

    const args = {
      description: "Test task",
      prompt: "Do something",
      subagent_name: "unknown-agent",
    };

    await expect(tool.preprocess!(args)).rejects.toThrow(
      "Unknown agent type: unknown-agent",
    );
    expect(vi.mocked(getSubagent)).toHaveBeenCalledWith(
      modelServiceState,
      "unknown-agent",
    );
  });

  it("preprocess includes agent model name when agent exists", async () => {
    vi.mocked(getAgentNames).mockReturnValue(["code-agent"]);

    const tool = await subagentTool();

    vi.mocked(getSubagent).mockReturnValue({
      model: { name: "test-model" },
    } as any);

    const args = {
      description: "Handle specialized task",
      prompt: "Do it",
      subagent_name: "code-agent",
    };

    const result = await tool.preprocess!(args);

    expect(result.preview).toEqual([
      {
        type: "text",
        content: "Spawning test-model to: Handle specialized task",
      },
    ]);
    expect(vi.mocked(getSubagent)).toHaveBeenCalledWith(
      modelServiceState,
      "code-agent",
    );
  });

  it("run executes subagent and returns formatted output", async () => {
    vi.mocked(getAgentNames).mockReturnValue(["code-agent"]);
    vi.mocked(getSubagent).mockReturnValue({
      model: { name: "test-model" },
    } as any);

    const execSpy = vi
      .spyOn(executor, "executeSubAgent")
      .mockResolvedValue({
      success: true,
      response: "subagent-output",
    } as any);

    const tool = await subagentTool();

    const result = await tool.run(
      {
        prompt: "Subagent prompt",
        subagent_name: "code-agent",
      },
      { toolCallId: "tool-call-id", parallelToolCallCount: 1 },
    );

    expect(execSpy).toHaveBeenCalledTimes(1);
    const [options] = execSpy.mock.calls[0];

    expect(options.prompt).toBe("Subagent prompt");
    expect(options.parentSessionId).toBe("parent-session-id");
    expect(typeof options.onOutputUpdate).toBe("function");

    options.onOutputUpdate?.("partial-output");
    expect(vi.mocked(services.chatHistory.addToolResult)).toHaveBeenCalledWith(
      "tool-call-id",
      "partial-output",
      "calling",
    );

    expect(result).toBe(
      "subagent-output\n<task_metadata>\nstatus: completed\n</task_metadata>",
    );

    execSpy.mockRestore();
  });
});


describe("executeSubAgent", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    vi.mocked(serviceContainer.get).mockResolvedValue({
      permissions: {
        policies: [],
      },
      currentMode: "default",
    } as any);
  });

  it("serializes concurrent executions to avoid global singleton desync", async () => {
    const order: string[] = [];
    let resolveFirst: (() => void) | undefined;
    const firstGate = new Promise<void>((resolve) => {
      resolveFirst = resolve;
    });

    let resolveSecond: (() => void) | undefined;
    const secondGate = new Promise<void>((resolve) => {
      resolveSecond = resolve;
    });

    let call = 0;
    vi.mocked(streamChatResponse).mockImplementation(async (chatHistory: any[]) => {
      call += 1;
      if (call === 1) {
        order.push("stream_1_start");
        await firstGate;
        order.push("stream_1_end");
        chatHistory.push({ message: { content: "r1" } });
        return;
      }

      order.push("stream_2_start");
      await secondGate;
      order.push("stream_2_end");
      chatHistory.push({ message: { content: "r2" } });
    });

    const agent = { model: { name: "m" }, llmApi: {} } as any;

    const p1 = executor.executeSubAgent({
      agent,
      prompt: "p1",
      parentSessionId: "s",
      abortController: new AbortController(),
    });

    for (let i = 0; i < 20; i += 1) {
      if (order.includes("stream_1_start")) {
        break;
      }
      // eslint-disable-next-line no-await-in-loop
      await Promise.resolve();
    }

    const p2 = executor.executeSubAgent({
      agent,
      prompt: "p2",
      parentSessionId: "s",
      abortController: new AbortController(),
    });

    for (let i = 0; i < 10; i += 1) {
      // eslint-disable-next-line no-await-in-loop
      await Promise.resolve();
    }

    expect(order).toEqual(["stream_1_start"]);

    resolveFirst?.();

    for (let i = 0; i < 20; i += 1) {
      if (order.includes("stream_2_start")) {
        break;
      }
      // eslint-disable-next-line no-await-in-loop
      await Promise.resolve();
    }

    resolveSecond?.();

    const [r1, r2] = await Promise.all([p1, p2]);
    expect(r1.success).toBe(true);
    expect(r2.success).toBe(true);
    expect(order).toEqual([
      "stream_1_start",
      "stream_1_end",
      "stream_2_start",
      "stream_2_end",
    ]);
  });
});
