import type { CSSProperties } from "react";

const fixture = {
  sentAt: "2026-08-25T13:00:00Z",
  header: "[CRITICAL] orders lag",
  summary: "Consumer group lag is above threshold.",
  fields: [
    ["Event", "firing"],
    ["Severity", "critical"],
    ["Metric", "consumer_group_lag"],
    ["Observed / threshold", "42 / > 10"],
  ],
  incident: "incident-1",
} as const;

const styles = {
  figure: {
    minWidth: 0,
    display: "grid",
    gridColumn: "1 / -1",
    gap: 9,
    margin: 0,
  },
  caption: {
    minWidth: 0,
    display: "grid",
    gridTemplateColumns: "minmax(0, 1fr) auto",
    gap: "3px 10px",
    alignItems: "start",
    padding: "10px 11px",
    border: "1px solid var(--border-soft)",
    borderRadius: 7,
    background: "#f6f7f9",
    color: "#4f5663",
    fontSize: 11,
    lineHeight: 1.5,
  },
  captionTitle: {
    color: "var(--text)",
    fontSize: 12,
  },
  captionCopy: {
    minWidth: 0,
    gridColumn: "1 / -1",
    margin: 0,
  },
  contract: {
    alignSelf: "center",
    padding: "3px 6px",
    border: "1px solid var(--border)",
    borderRadius: 4,
    background: "#fff",
    color: "#333942",
    fontSize: 10,
    whiteSpace: "normal",
    overflowWrap: "anywhere",
  },
  frame: {
    minWidth: 0,
    overflow: "hidden",
    border: "1px solid #c8c7ca",
    borderRadius: 8,
    background: "#fff",
    boxShadow: "0 1px 2px rgba(29, 28, 29, 0.08)",
    color: "#1d1c1d",
    fontFamily: "Arial, Helvetica, sans-serif",
  },
  channelHeader: {
    minHeight: 38,
    display: "flex",
    alignItems: "center",
    justifyContent: "space-between",
    gap: 10,
    padding: "0 13px",
    borderBottom: "1px solid #dddddd",
    background: "#fafafa",
    fontSize: 12,
  },
  channelMeta: {
    color: "#616061",
    fontSize: 10,
  },
  message: {
    minWidth: 0,
    display: "grid",
    gridTemplateColumns: "36px minmax(0, 1fr)",
    gap: 10,
    padding: "13px",
  },
  avatar: {
    width: 36,
    height: 36,
    display: "grid",
    placeItems: "center",
    borderRadius: 7,
    background: "#4a154b",
    color: "#fff",
    fontSize: 13,
    fontWeight: 800,
    letterSpacing: "-0.04em",
  },
  messageBody: {
    minWidth: 0,
    display: "grid",
    gap: 8,
  },
  sender: {
    minWidth: 0,
    display: "flex",
    flexWrap: "wrap",
    alignItems: "baseline",
    gap: 5,
    fontSize: 12,
  },
  appBadge: {
    padding: "1px 4px",
    borderRadius: 3,
    background: "#1264a3",
    color: "#fff",
    fontSize: 8,
    fontWeight: 700,
    lineHeight: 1.5,
  },
  time: {
    color: "#616061",
    fontSize: 10,
  },
  alertHeader: {
    margin: 0,
    fontSize: 15,
    lineHeight: 1.35,
  },
  summary: {
    margin: 0,
    color: "#3f3e40",
    fontSize: 12,
    lineHeight: 1.5,
    overflowWrap: "anywhere",
  },
  fieldGrid: {
    minWidth: 0,
    display: "grid",
    gap: "9px 14px",
    margin: 0,
    padding: "10px 0 0",
    borderTop: "1px solid #e7e7e7",
  },
  field: {
    minWidth: 0,
    display: "grid",
    gap: 3,
  },
  term: {
    color: "#1d1c1d",
    fontSize: 11,
    fontWeight: 700,
  },
  detail: {
    minWidth: 0,
    margin: 0,
    color: "#3f3e40",
    fontSize: 11,
    overflowWrap: "anywhere",
  },
  code: {
    padding: "1px 4px",
    borderRadius: 3,
    background: "#f1f1f1",
    color: "#1d1c1d",
    fontFamily: "ui-monospace, SFMono-Regular, Menlo, Consolas, monospace",
    fontSize: 10,
    whiteSpace: "normal",
    overflowWrap: "anywhere",
  },
  fullField: {
    minWidth: 0,
    display: "grid",
    gridColumn: "1 / -1",
    gap: 4,
  },
  scope: {
    minWidth: 0,
    display: "flex",
    flexWrap: "wrap",
    gap: "4px 8px",
    color: "#3f3e40",
    fontSize: 11,
  },
  context: {
    marginTop: 2,
    paddingTop: 8,
    borderTop: "1px solid #e7e7e7",
    color: "#616061",
    fontSize: 10,
    overflowWrap: "anywhere",
  },
  actions: {
    minWidth: 0,
    display: "flex",
    flexWrap: "wrap",
    alignItems: "center",
    gap: "7px 9px",
    paddingTop: 2,
  },
  actionButton: {
    minHeight: 30,
    display: "inline-flex",
    alignItems: "center",
    padding: "0 11px",
    border: "1px solid #c8c7ca",
    borderRadius: 4,
    background: "#fff",
    color: "#1264a3",
    fontSize: 11,
    fontWeight: 700,
    boxShadow: "0 1px 1px rgba(29, 28, 29, 0.04)",
  },
  actionHint: {
    minWidth: 0,
    color: "#616061",
    fontSize: 9,
    overflowWrap: "anywhere",
  },
} satisfies Record<string, CSSProperties>;

export function SlackAlertPreview() {
  return <figure className="slack-alert-preview" aria-label="Example Slack alert delivery" style={styles.figure}>
    <figcaption style={styles.caption}>
      <strong style={styles.captionTitle}>Slack message example</strong>
      <code style={styles.contract}>POST application/json · text + blocks</code>
      <p style={styles.captionCopy}>RedisStreamScope posts a text fallback and matching Slack blocks to the Incoming Webhook URL. The <strong>Event</strong> field identifies the delivery as firing, repeat, resolved, escalation, or test.</p>
      <p style={styles.captionCopy}>The evidence button is included when the app&apos;s public address is configured with <code>PUBLIC_URL</code> or <code>server.publicURL</code>. Slack supplies the channel, app name, and avatar from the Incoming Webhook configuration.</p>
    </figcaption>
    <section aria-label="Rendered Slack message preview" style={styles.frame}>
      <header style={styles.channelHeader}><strong># stream-alerts</strong><span style={styles.channelMeta}>Example preview</span></header>
      <article style={styles.message}>
        <div aria-hidden="true" style={styles.avatar}>RS</div>
        <div style={styles.messageBody}>
          <div style={styles.sender}><strong>RedisStreamScope</strong><span style={styles.appBadge}>APP</span><time dateTime={fixture.sentAt} style={styles.time}>1:00 PM</time></div>
          <h3 style={styles.alertHeader}>{fixture.header}</h3>
          <p style={styles.summary}>{fixture.summary}</p>
          <dl className="slack-alert-preview__fields" style={styles.fieldGrid}>
            {fixture.fields.map(([label, value]) => <div key={label} style={styles.field}><dt style={styles.term}>{label}</dt><dd style={styles.detail}><code style={styles.code}>{value}</code></dd></div>)}
            <div style={styles.fullField}>
              <dt style={styles.term}>Scope</dt>
              <dd style={{ ...styles.detail, ...styles.scope }}>
                <span><strong>connection:</strong> <code style={styles.code}>redis</code></span>
                <span aria-hidden="true">•</span>
                <span><strong>stream:</strong> <code style={styles.code}>orders</code></span>
                <span aria-hidden="true">•</span>
                <span><strong>group:</strong> <code style={styles.code}>workers</code></span>
              </dd>
            </div>
            <div style={styles.fullField}><dt style={styles.term}>Incident</dt><dd style={styles.detail}><code style={styles.code}>{fixture.incident}</code></dd></div>
          </dl>
          <div style={styles.actions} aria-label="Slack Block Kit action example">
            <span style={styles.actionButton}>View alert evidence</span>
            <span style={styles.actionHint}>Opens RedisStreamScope · Alerts · {fixture.incident}</span>
          </div>
          <footer style={styles.context}>RedisStreamScope • <time dateTime={fixture.sentAt}>{fixture.sentAt}</time></footer>
        </div>
      </article>
    </section>
  </figure>;
}
