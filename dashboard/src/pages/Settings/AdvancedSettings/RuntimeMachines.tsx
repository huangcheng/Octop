/**
 * Remote machines — enrollment management for remote runtime backends
 * (spec §7.1): issue / rotate / revoke `rk_` credentials, online state.
 * Keys are shown exactly once; rotation is single-slot (409 while pending).
 */

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import {
  Alert,
  Badge,
  Button,
  Checkbox,
  Form,
  Input,
  Modal,
  Popconfirm,
  Space,
  Spin,
  Table,
  Tag,
  Typography,
} from "antd";
import type { ColumnsType } from "antd/es/table";
import { message } from "@/utils/antdMessage";
import {
  Copy,
  KeyRound,
  MonitorSmartphone,
  Plus,
  RefreshCw,
  RotateCw,
  ShieldAlert,
  Trash2,
} from "lucide-react";
import { useTranslation } from "react-i18next";

import { useServerTimezone } from "../../../hooks/useServerTimezone";
import { copyText } from "../../../utils/copyText";
import { apiErrorMessage } from "../../../utils/apiError";
import { formatServerDateTime } from "../../../utils/formatMessageTime";
import {
  runtimesApi,
  type IssuedRuntimeEnrollment,
  type RuntimeEnrollment,
} from "../../../api/modules/runtimes";
import { TabPanelHeader } from "./TabPanelHeader";
import styles from "./RuntimeMachines.module.less";

const { Text } = Typography;

const PREFIX = "advancedSettings.runtimeMachines";

/** Tag styling per enrollment status; unknown statuses render raw. */
const STATUS_VIEWS: Record<string, { color: string; key: string }> = {
  active: { color: "green", key: `${PREFIX}.statusActive` },
  revoked: { color: "default", key: `${PREFIX}.statusRevoked` },
};

/** Silent online refresh cadence (list + one status call per row). */
const ONLINE_POLL_MS = 15000;

/** New key shown once after a successful rotation. */
interface KeyReveal {
  key: string;
  name: string;
}

export default function RuntimeMachinesPanel() {
  const { t } = useTranslation();
  const timeZone = useServerTimezone();
  const [loading, setLoading] = useState(true);
  const [rows, setRows] = useState<RuntimeEnrollment[]>([]);
  const [onlineById, setOnlineById] = useState<Record<string, boolean>>({});
  const [busyId, setBusyId] = useState<string | null>(null);
  const [issueOpen, setIssueOpen] = useState(false);
  const [issuing, setIssuing] = useState(false);
  const [issueResult, setIssueResult] =
    useState<IssuedRuntimeEnrollment | null>(null);
  const [keyReveal, setKeyReveal] = useState<KeyReveal | null>(null);
  const [issueForm] = Form.useForm();
  const nameValue = Form.useWatch("display_name", issueForm);
  const allowExecValue = Form.useWatch("allow_exec", issueForm);
  const consentValue = Form.useWatch("exec_consent", issueForm);

  const reloadingRef = useRef(false);
  const reload = useCallback(
    async (opts?: { silent?: boolean }) => {
      const silent = Boolean(opts?.silent);
      // Concurrency guard: overlapping waves race their setRows/setOnlineById
      // and the slower wave resurrects stale online flags.
      if (reloadingRef.current) return;
      reloadingRef.current = true;
      if (!silent) setLoading(true);
      try {
        const list = await runtimesApi.list();
        setRows(list);
        // The list view carries no online flag — probe each enrollment's
        // status endpoint (live-session registry), the same pattern
        // BridgeSettings uses for per-connection agent lists.
        const online: Record<string, boolean> = {};
        await Promise.all(
          list.map(async (row) => {
            try {
              const status = await runtimesApi.status(row.enrollment_id);
              online[row.enrollment_id] = Boolean(status.online);
            } catch {
              online[row.enrollment_id] = false;
            }
          }),
        );
        setOnlineById(online);
      } catch (err) {
        if (!silent) {
          message.error(apiErrorMessage(err, t(`${PREFIX}.loadFailed`), t));
        }
      } finally {
        reloadingRef.current = false;
        if (!silent) setLoading(false);
      }
    },
    [t],
  );

  // Latest-ref: the initial load + poll must not re-run when `t` changes
  // identity between renders (react-i18next mocks do that in tests).
  const reloadRef = useRef(reload);
  useEffect(() => {
    reloadRef.current = reload;
  }, [reload]);

  useEffect(() => {
    void reloadRef.current();
    const timer = window.setInterval(() => {
      void reloadRef.current({ silent: true });
    }, ONLINE_POLL_MS);
    return () => window.clearInterval(timer);
  }, []);

  const onCopy = useCallback(
    async (value: string) => {
      const ok = await copyText(value);
      if (ok) {
        message.success(t(`${PREFIX}.copied`));
      } else {
        message.warning(t(`${PREFIX}.copyFailed`));
      }
    },
    [t],
  );

  const closeIssue = () => {
    setIssueOpen(false);
    setIssueResult(null);
    issueForm.resetFields();
    void reload({ silent: true });
  };

  const onIssue = async () => {
    try {
      const values = await issueForm.validateFields();
      setIssuing(true);
      const result = await runtimesApi.issue({
        display_name: String(values.display_name).trim(),
        allow_exec: Boolean(values.allow_exec),
      });
      setIssueResult(result);
    } catch (err) {
      if (err && typeof err === "object" && "errorFields" in err) return;
      message.error(apiErrorMessage(err, t(`${PREFIX}.issueFailed`), t));
    } finally {
      setIssuing(false);
    }
  };

  const onRotate = async (row: RuntimeEnrollment) => {
    setBusyId(row.enrollment_id);
    try {
      const result = await runtimesApi.rotate(row.enrollment_id);
      setKeyReveal({ key: result.key_shown_once, name: row.display_name });
    } catch (err) {
      // 409 RUNTIME_ROTATION_PENDING and friends map to apiErrors.* copy.
      message.error(apiErrorMessage(err, t(`${PREFIX}.rotateFailed`), t));
    } finally {
      setBusyId(null);
      void reload({ silent: true });
    }
  };

  const onRevoke = async (row: RuntimeEnrollment) => {
    setBusyId(row.enrollment_id);
    try {
      await runtimesApi.revoke(row.enrollment_id);
      message.success(t(`${PREFIX}.revoked`));
    } catch (err) {
      message.error(apiErrorMessage(err, t(`${PREFIX}.revokeFailed`), t));
    } finally {
      setBusyId(null);
      void reload({ silent: true });
    }
  };

  // Consent gate: exec requires the explicit acknowledgement checkbox.
  const issueBlocked =
    !String(nameValue ?? "").trim() ||
    (Boolean(allowExecValue) && !consentValue);

  const tableColumns: ColumnsType<RuntimeEnrollment> = [
    {
      title: t(`${PREFIX}.colName`),
      key: "display_name",
      width: 200,
      render: (_: unknown, row) => (
        <div className={styles.nameCell}>
          <Text strong ellipsis>
            {row.display_name}
          </Text>
          {row.default_workspace_base ? (
            <Text type="secondary" ellipsis className={styles.subline}>
              {row.default_workspace_base}
            </Text>
          ) : null}
        </div>
      ),
    },
    {
      title: t(`${PREFIX}.colMachineId`),
      dataIndex: "machine_id",
      key: "machine_id",
      width: 190,
      render: (machineId: string | null) =>
        machineId ? (
          <Text code copyable className={styles.machineId}>
            {machineId}
          </Text>
        ) : (
          <Text type="secondary">{t(`${PREFIX}.machineIdPending`)}</Text>
        ),
    },
    {
      title: t(`${PREFIX}.colStatus`),
      key: "status",
      width: 210,
      render: (_: unknown, row) => {
        const online = onlineById[row.enrollment_id];
        const view = STATUS_VIEWS[row.status];
        return (
          <Space direction="vertical" size={2}>
            <Space size={8}>
              <Badge
                status={online ? "success" : "default"}
                text={t(online ? `${PREFIX}.online` : `${PREFIX}.offline`)}
              />
              <Tag color={view?.color ?? "default"}>
                {view ? t(view.key) : row.status}
              </Tag>
            </Space>
            {row.rotation_pending ? (
              <Tag color="orange" className={styles.rotationTag}>
                {t(`${PREFIX}.rotationPending`)}{" "}
                {row.rotation_expires_at
                  ? formatServerDateTime(row.rotation_expires_at, timeZone)
                  : ""}
              </Tag>
            ) : null}
          </Space>
        );
      },
    },
    {
      title: t(`${PREFIX}.colLastSeen`),
      dataIndex: "last_seen_at",
      key: "last_seen_at",
      width: 180,
      render: (ts: number | null) => (
        <Text type="secondary">
          {ts ? formatServerDateTime(ts, timeZone) : t(`${PREFIX}.neverSeen`)}
        </Text>
      ),
    },
    {
      title: t(`${PREFIX}.colExec`),
      dataIndex: "allow_exec",
      key: "allow_exec",
      width: 130,
      render: (allowExec: boolean) =>
        allowExec ? (
          <Tag color="red">{t(`${PREFIX}.execAllowed`)}</Tag>
        ) : (
          <Tag>{t(`${PREFIX}.execDisabled`)}</Tag>
        ),
    },
    {
      title: t(`${PREFIX}.colActions`),
      key: "actions",
      width: 190,
      render: (_: unknown, row) => {
        const busy = busyId === row.enrollment_id;
        const revoked = row.status === "revoked";
        // A pending rotation must be consumed (or expire) first — the hub
        // answers a second rotate with 409 RUNTIME_ROTATION_PENDING.
        const rotateBlocked = revoked || row.rotation_pending;
        return (
          <Space size={4}>
            <Popconfirm
              title={t(`${PREFIX}.rotateConfirmTitle`)}
              description={t(`${PREFIX}.rotateConfirmBody`, {
                name: row.display_name,
              })}
              okText={t(`${PREFIX}.rotateConfirmOk`)}
              cancelText={t("common.cancel")}
              disabled={rotateBlocked}
              onConfirm={() => void onRotate(row)}
            >
              <Button
                size="small"
                type="text"
                disabled={rotateBlocked}
                loading={busy}
                icon={<RotateCw size={14} />}
              >
                {t(`${PREFIX}.rotate`)}
              </Button>
            </Popconfirm>
            <Popconfirm
              title={t(`${PREFIX}.revokeConfirmTitle`)}
              description={t(`${PREFIX}.revokeConfirmBody`, {
                name: row.display_name,
              })}
              okText={t(`${PREFIX}.revokeConfirmOk`)}
              cancelText={t("common.cancel")}
              okButtonProps={{ danger: true }}
              disabled={revoked}
              onConfirm={() => void onRevoke(row)}
            >
              <Button
                size="small"
                type="text"
                danger
                disabled={revoked}
                loading={busy}
                icon={<Trash2 size={14} />}
              >
                {t(`${PREFIX}.revoke`)}
              </Button>
            </Popconfirm>
          </Space>
        );
      },
    },
  ];
  // Memoized: the 15s poll replaces onlineById and would otherwise re-diff
  // the column definitions every cycle (BridgeSettings memoizes likewise).
  const columns: ColumnsType<RuntimeEnrollment> = useMemo(
    () => tableColumns,
    [onlineById, busyId, t, timeZone],
  );

  return (
    <>
      <TabPanelHeader
        icon={<MonitorSmartphone size={18} strokeWidth={1.8} />}
        title={t(`${PREFIX}.title`)}
        description={t(`${PREFIX}.description`)}
        actions={
          <>
            <Button
              icon={<RefreshCw size={14} />}
              onClick={() => void reload()}
            >
              {t(`${PREFIX}.refresh`)}
            </Button>
            <Button
              type="primary"
              icon={<Plus size={14} />}
              onClick={() => setIssueOpen(true)}
            >
              {t(`${PREFIX}.issue`)}
            </Button>
          </>
        }
      />

      {loading ? (
        <div className={styles.loading}>
          <Spin />
        </div>
      ) : (
        <Table<RuntimeEnrollment>
          rowKey="enrollment_id"
          size="middle"
          pagination={false}
          columns={columns}
          dataSource={rows}
          scroll={{ x: 900 }}
        />
      )}

      <Modal
        open={issueOpen}
        title={t(`${PREFIX}.issueTitle`)}
        okText={issueResult ? t(`${PREFIX}.done`) : t(`${PREFIX}.issueSubmit`)}
        cancelText={t("common.cancel")}
        cancelButtonProps={{
          disabled: issuing,
          // Once the key is on screen, only one close button stays.
          hidden: issueResult !== null,
        }}
        okButtonProps={{
          disabled: !issueResult && issueBlocked,
          loading: issuing,
        }}
        onOk={() => (issueResult ? closeIssue() : void onIssue())}
        onCancel={closeIssue}
        destroyOnHidden
        width={560}
      >
        {issueResult ? (
          <div className={styles.keyResult}>
            <Alert
              type="warning"
              showIcon
              icon={<KeyRound size={16} />}
              message={t(`${PREFIX}.keyShownOnceTitle`, {
                name: issueResult.display_name,
              })}
              description={t(`${PREFIX}.keyShownOnceWarning`)}
            />
            <div className={styles.keyRow}>
              <Text code className={styles.keyCode}>
                {issueResult.key_shown_once}
              </Text>
              <Button
                icon={<Copy size={14} />}
                onClick={() => void onCopy(issueResult.key_shown_once)}
              >
                {t(`${PREFIX}.copyKey`)}
              </Button>
            </div>
            <div className={styles.commandTitle}>
              {t(`${PREFIX}.enrollCommand`)}
            </div>
            <Text code className={styles.commandCode}>
              {issueResult.enroll_command}
            </Text>
            <Typography.Paragraph
              type="secondary"
              className={styles.commandHint}
            >
              {t(`${PREFIX}.enrollCommandHint`)}
            </Typography.Paragraph>
          </div>
        ) : (
          <Form
            form={issueForm}
            layout="vertical"
            requiredMark={false}
            className={styles.issueForm}
          >
            <Form.Item
              name="display_name"
              label={t(`${PREFIX}.issueName`)}
              rules={[
                {
                  required: true,
                  whitespace: true,
                  message: t(`${PREFIX}.issueNameRequired`),
                },
                { max: 64 },
              ]}
            >
              <Input
                placeholder={t(`${PREFIX}.issueNamePlaceholder`)}
                maxLength={64}
                autoFocus
              />
            </Form.Item>
            <Form.Item
              name="allow_exec"
              valuePropName="checked"
              initialValue={false}
              extra={t(`${PREFIX}.allowExecHint`)}
            >
              <Checkbox>{t(`${PREFIX}.allowExec`)}</Checkbox>
            </Form.Item>
            {allowExecValue ? (
              <>
                <Alert
                  type="warning"
                  showIcon
                  icon={<ShieldAlert size={16} />}
                  className={styles.consentAlert}
                  message={t(`${PREFIX}.allowExecWarningTitle`)}
                  description={t(`${PREFIX}.allowExecWarning`)}
                />
                <Form.Item
                  name="exec_consent"
                  valuePropName="checked"
                  initialValue={false}
                  className={styles.consentItem}
                >
                  <Checkbox>{t(`${PREFIX}.consent`)}</Checkbox>
                </Form.Item>
              </>
            ) : null}
          </Form>
        )}
      </Modal>

      <Modal
        open={keyReveal !== null}
        title={t(`${PREFIX}.rotateResultTitle`, {
          name: keyReveal?.name ?? "",
        })}
        footer={
          <Button type="primary" onClick={() => setKeyReveal(null)}>
            {t(`${PREFIX}.done`)}
          </Button>
        }
        onCancel={() => setKeyReveal(null)}
        destroyOnHidden
        width={560}
      >
        {keyReveal ? (
          <div className={styles.keyResult}>
            <Alert
              type="warning"
              showIcon
              icon={<KeyRound size={16} />}
              message={t(`${PREFIX}.keyShownOnceWarning`)}
              description={t(`${PREFIX}.rotateResultHint`)}
            />
            <div className={styles.keyRow}>
              <Text code className={styles.keyCode}>
                {keyReveal.key}
              </Text>
              <Button
                icon={<Copy size={14} />}
                onClick={() => void onCopy(keyReveal.key)}
              >
                {t(`${PREFIX}.copyKey`)}
              </Button>
            </div>
          </div>
        ) : null}
      </Modal>
    </>
  );
}
