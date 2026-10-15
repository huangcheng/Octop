import { beforeEach, describe, expect, it, vi } from "vitest";
import { render, screen, waitFor, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";

const { list, issue, rotate, revoke, status, messageError } = vi.hoisted(
  () => ({
    list: vi.fn(),
    issue: vi.fn(),
    rotate: vi.fn(),
    revoke: vi.fn(),
    status: vi.fn(),
    messageError: vi.fn(),
  }),
);

vi.mock("../../../api/modules/runtimes", () => ({
  runtimesApi: { list, issue, rotate, revoke, status },
}));

vi.mock("../../../hooks/useServerTimezone", () => ({
  useServerTimezone: () => "Asia/Shanghai",
}));

vi.mock("@/utils/antdMessage", () => ({
  message: { error: messageError, success: vi.fn(), warning: vi.fn() },
}));

import RuntimeMachinesPanel from "./RuntimeMachines";

const P = "advancedSettings.runtimeMachines";

const ENROLLMENT = {
  enrollment_id: "01J8RK",
  machine_id: "01JMACHINE",
  display_name: "MacBook Pro",
  allow_exec: false,
  default_workspace_base: null,
  status: "active",
  generation: 2,
  last_seen_at: 1760000000,
  rotation_pending: false,
  rotation_expires_at: null,
  machine_metadata: null,
  created_at: 1759000000,
  updated_at: 1759000000,
};

describe("<RuntimeMachinesPanel />", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    list.mockResolvedValue([ENROLLMENT]);
    status.mockResolvedValue({ ...ENROLLMENT, online: true });
  });

  it("gates the issue modal on the exec consent before allowing submit", async () => {
    const user = userEvent.setup();
    issue.mockResolvedValue({
      ...ENROLLMENT,
      display_name: "Studio Mac",
      enroll_command:
        "octop runtime enroll --server https://hub.example --key-file <path-to-key-file> --ca-bundle <path-to-ca-bundle>",
      key_shown_once: "rk_test_shown_once",
    });

    render(<RuntimeMachinesPanel />);
    await waitFor(() =>
      expect(screen.getByText("MacBook Pro")).toBeInTheDocument(),
    );

    await user.click(screen.getByRole("button", { name: `${P}.issue` }));
    const modal = await screen.findByRole("dialog");

    // No exec requested yet → no consent copy, submit only blocked by name.
    expect(screen.queryByText(`${P}.allowExecWarning`)).not.toBeInTheDocument();

    const submit = within(modal).getByRole("button", {
      name: `${P}.issueSubmit`,
    });
    expect(submit).toBeDisabled();

    // Enabling exec reveals the explicit warning + consent checkbox and
    // keeps submit disabled until the consent is acknowledged.
    await user.click(screen.getByRole("checkbox", { name: `${P}.allowExec` }));
    expect(screen.getByText(`${P}.allowExecWarningTitle`)).toBeInTheDocument();
    expect(screen.getByText(`${P}.allowExecWarning`)).toBeInTheDocument();
    expect(screen.getByText(`${P}.consent`)).toBeInTheDocument();
    expect(submit).toBeDisabled();

    await user.click(screen.getByRole("checkbox", { name: `${P}.consent` }));
    expect(submit).toBeDisabled(); // still no machine name

    await user.type(
      screen.getByPlaceholderText(`${P}.issueNamePlaceholder`),
      "Studio Mac",
    );
    expect(submit).toBeEnabled();

    await user.click(submit);
    await waitFor(() =>
      expect(issue).toHaveBeenCalledWith({
        display_name: "Studio Mac",
        allow_exec: true,
      }),
    );

    // Result view: the key is shown exactly once, with the enroll command.
    expect(await screen.findByText("rk_test_shown_once")).toBeInTheDocument();
    expect(
      screen.getByText(/octop runtime enroll --server https:\/\/hub\.example/),
    ).toBeInTheDocument();
    expect(
      within(modal).getByRole("button", { name: `${P}.copyKey` }),
    ).toBeInTheDocument();
  });

  it("maps a rotate 409 RUNTIME_ROTATION_PENDING through apiErrorMessage", async () => {
    const user = userEvent.setup();
    rotate.mockRejectedValue(
      new Error(
        '409 Conflict - {"error":{"code":"RUNTIME_ROTATION_PENDING","message":"rotation already pending; use or wait for it to expire"}}',
      ),
    );

    render(<RuntimeMachinesPanel />);
    await waitFor(() =>
      expect(screen.getByText("MacBook Pro")).toBeInTheDocument(),
    );

    await user.click(screen.getByRole("button", { name: `${P}.rotate` }));
    await user.click(
      await screen.findByRole("button", { name: `${P}.rotateConfirmOk` }),
    );

    // The shared apiError util parses the envelope; with the test t mock the
    // server message wins over the fallback text.
    await waitFor(() =>
      expect(messageError).toHaveBeenCalledWith(
        "rotation already pending; use or wait for it to expire",
      ),
    );
    // No key reveal modal for a failed rotation.
    expect(
      screen.queryByText(`${P}.rotateResultTitle`),
    ).not.toBeInTheDocument();
  });
});
