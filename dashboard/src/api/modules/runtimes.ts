import { request } from "../request";

/**
 * Remote runtime enrollments — hub-side view of machines enrolled via
 * ``octop runtime enroll``. Key hashes and conflict telemetry never leave
 * the server (see ``_public`` in api/routers/runtimes.py).
 */
export interface RuntimeEnrollment {
  enrollment_id: string;
  /** Daemon-generated ULID; null until the machine completes its first hello. */
  machine_id: string | null;
  display_name: string;
  allow_exec: boolean;
  default_workspace_base: string | null;
  /** ``active`` | ``revoked`` (rotation mutates the same row). */
  status: string;
  generation: number;
  /** Epoch seconds; null when the machine has never connected. */
  last_seen_at: number | null;
  /** True while an issued rotation key is still unconsumed. */
  rotation_pending: boolean;
  rotation_expires_at: number | null;
  /** Raw JSON string from the DB (schema-versioned; unknown versions ignored). */
  machine_metadata: string | null;
  created_at: number;
  updated_at: number;
}

/** GET /{id}/status — list rows plus the live-session online flag. */
export interface RuntimeEnrollmentStatus extends RuntimeEnrollment {
  online: boolean;
}

/** POST /runtimes — the plaintext key is returned exactly once. */
export interface IssuedRuntimeEnrollment extends RuntimeEnrollment {
  enroll_command: string;
  key_shown_once: string;
}

/** POST /{id}/rotate — new plaintext key, shown exactly once. */
export interface RotatedRuntimeEnrollment extends RuntimeEnrollment {
  key_shown_once: string;
}

export interface IssueRuntimeBody {
  display_name: string;
  allow_exec?: boolean;
  default_workspace_base?: string | null;
}

export const runtimesApi = {
  list: () => request<RuntimeEnrollment[]>("/runtimes"),

  issue: (body: IssueRuntimeBody) =>
    request<IssuedRuntimeEnrollment>("/runtimes", {
      method: "POST",
      body: JSON.stringify(body),
    }),

  rotate: (enrollmentId: string) =>
    request<RotatedRuntimeEnrollment>(
      `/runtimes/${encodeURIComponent(enrollmentId)}/rotate`,
      { method: "POST" },
    ),

  revoke: (enrollmentId: string) =>
    request<RuntimeEnrollment>(
      `/runtimes/${encodeURIComponent(enrollmentId)}`,
      {
        method: "DELETE",
      },
    ),

  status: (enrollmentId: string) =>
    request<RuntimeEnrollmentStatus>(
      `/runtimes/${encodeURIComponent(enrollmentId)}/status`,
    ),
};
