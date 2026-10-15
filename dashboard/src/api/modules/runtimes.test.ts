import { beforeEach, describe, expect, it, vi } from "vitest";
import { apiErrorMessage } from "../../utils/apiError";

const { request } = vi.hoisted(() => ({ request: vi.fn() }));

vi.mock("../request", () => ({ request }));

import { runtimesApi } from "./runtimes";

describe("runtimesApi", () => {
  beforeEach(() => {
    request.mockReset();
  });

  it("uses the enrollment management endpoints", async () => {
    request.mockResolvedValue([]);

    await runtimesApi.list();
    await runtimesApi.issue({
      display_name: "MacBook Pro",
      allow_exec: true,
    });
    await runtimesApi.rotate("01J8RK");
    await runtimesApi.revoke("01J8RK");
    await runtimesApi.status("01J8RK");

    expect(request).toHaveBeenNthCalledWith(1, "/runtimes");
    expect(request).toHaveBeenNthCalledWith(2, "/runtimes", {
      method: "POST",
      body: JSON.stringify({ display_name: "MacBook Pro", allow_exec: true }),
    });
    expect(request).toHaveBeenNthCalledWith(3, "/runtimes/01J8RK/rotate", {
      method: "POST",
    });
    expect(request).toHaveBeenNthCalledWith(4, "/runtimes/01J8RK", {
      method: "DELETE",
    });
    expect(request).toHaveBeenNthCalledWith(5, "/runtimes/01J8RK/status");
  });

  it("encodes enrollment ids in path parameters", async () => {
    request.mockResolvedValue(null);

    await runtimesApi.rotate("01J8/RK ?");
    expect(request).toHaveBeenCalledWith("/runtimes/01J8%2FRK%20%3F/rotate", {
      method: "POST",
    });
  });

  it("maps thrown error envelopes through the shared apiError util", async () => {
    // Shape thrown by request(): message carries the raw response body.
    request.mockRejectedValue(
      new Error(
        '409 Conflict - {"error":{"code":"RUNTIME_ROTATION_PENDING","message":"rotation already pending"}}',
      ),
    );

    let caught: unknown;
    try {
      await runtimesApi.rotate("01J8RK");
    } catch (err) {
      caught = err;
    }
    expect(caught).toBeInstanceOf(Error);

    // A TFunction that resolves apiErrors.* like the real i18n bundles do.
    const translate: Record<string, string> = {
      "apiErrors.RUNTIME_ROTATION_PENDING":
        "A credential rotation is already pending for this machine.",
    };

    // A server message that says more than the generic code copy wins.
    expect(
      apiErrorMessage(caught, "fallback", (key) => translate[key] ?? key),
    ).toBe("rotation already pending");
    // Without a server message the code maps to the apiErrors.* copy…
    expect(
      apiErrorMessage(
        new Error('409 - {"error":{"code":"RUNTIME_ROTATION_PENDING"}}'),
        "fallback",
        (key) => translate[key] ?? key,
      ),
    ).toBe("A credential rotation is already pending for this machine.");
    // …and with neither translation nor server message, the fallback stands.
    expect(apiErrorMessage(caught, "fallback")).toBe(
      "rotation already pending",
    );
  });
});
