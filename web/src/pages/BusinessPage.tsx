import { useCallback, useEffect, useState } from "react";
import { AlertTriangle, RefreshCw } from "lucide-react";
import { Badge } from "@nous-research/ui/ui/components/badge";
import { Button } from "@nous-research/ui/ui/components/button";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "@nous-research/ui/ui/components/card";
import { Spinner } from "@nous-research/ui/ui/components/spinner";
import { api, type BusinessStatusResponse } from "@/lib/api";

function money(value = 0, currency = "USD"): string {
  return new Intl.NumberFormat(undefined, {
    style: "currency",
    currency,
  }).format(value / 100);
}

function measured(value: number, scale: number, unit: string): string {
  const actual = value / scale;
  return unit === "ratio"
    ? `${new Intl.NumberFormat(undefined, { style: "percent", maximumFractionDigits: 2 }).format(actual)}`
    : `${new Intl.NumberFormat().format(actual)} ${unit}`;
}

/** Seconds elapsed since a Unix epoch-seconds timestamp. Kept as a plain
 * module-level function (not inlined in JSX) so the impure Date.now() read
 * happens outside the component's render body. */
function secondsSince(epochSeconds: number): number {
  return Math.max(0, Math.floor(Date.now() / 1000) - epochSeconds);
}

export default function BusinessPage() {
  const [data, setData] = useState<BusinessStatusResponse | null>(null);
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(true);

  const load = useCallback(async () => {
    setLoading(true);
    setError("");
    try {
      setData(await api.getBusinessStatus());
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : String(reason));
    } finally {
      setLoading(false);
    }
  }, []);

  useEffect(() => {
    let cancelled = false;
    api.getBusinessStatus()
      .then((value) => {
        if (!cancelled) setData(value);
      })
      .catch((reason) => {
        if (!cancelled) {
          setError(reason instanceof Error ? reason.message : String(reason));
        }
      })
      .finally(() => {
        if (!cancelled) setLoading(false);
      });
    return () => {
      cancelled = true;
    };
  }, []);

  if (loading && !data) {
    return <div className="flex flex-1 items-center justify-center"><Spinner /></div>;
  }
  if (error) {
    return <div className="p-6 text-destructive">{error}</div>;
  }
  if (!data?.configured) {
    return (
      <div className="p-6">
        <Card><CardContent className="py-6">{data?.reason}</CardContent></Card>
      </div>
    );
  }

  const currency = data.treasury?.currency ?? "USD";
  const setAutonomy = async (mode: "autonomous" | "paused" | "manual") => {
    await api.setBusinessAutonomy(
      mode,
      mode === "autonomous"
        ? "Operator resumed bounded autonomous operation"
        : "Operator activated dashboard master control",
    );
    await load();
  };
  const resolveIntervention = async (interventionId: string, optionId: string) => {
    await api.resolveBusinessIntervention(interventionId, optionId, {
      source: "business_dashboard",
      reviewed_context: true,
      recorded_at: new Date().toISOString(),
    });
    await load();
  };
  return (
    <div className="flex-1 overflow-auto p-6 space-y-6">
      <div className="flex items-start justify-between gap-4">
        <div>
          <h1 className="text-2xl font-semibold">{data.organization?.name}</h1>
          <p className="text-sm text-muted-foreground">{data.decision_memo?.headline}</p>
        </div>
        <Button ghost onClick={() => void load()} aria-label="Refresh business status">
          <RefreshCw className="h-4 w-4" /> Refresh
        </Button>
        {data.autonomy?.mode === "autonomous" ? (
          <Button destructive onClick={() => void setAutonomy("paused")}>
            Pause autonomy
          </Button>
        ) : (
          <Button onClick={() => void setAutonomy("autonomous")}>
            Resume bounded autonomy
          </Button>
        )}
      </div>

      <Card>
        <CardHeader><CardTitle>Autonomous operation readiness</CardTitle></CardHeader>
        <CardContent className="space-y-3">
          <div className="flex items-center justify-between gap-3">
            <p className="text-sm text-muted-foreground">
              Control-plane admissibility is separate from worker liveness.
            </p>
            <Badge tone={data.readiness?.ready ? "secondary" : "destructive"}>
              {data.readiness?.ready ? "ready" : data.readiness?.state ?? "unknown"}
            </Badge>
          </div>
          {(data.readiness?.blockers.length ?? 0) > 0 && (
            <ul className="list-disc space-y-1 pl-5 text-sm text-destructive">
              {data.readiness?.blockers.map((blocker) => (
                <li key={blocker.code}>
                  <span className="font-mono">{blocker.code}</span>: {blocker.summary}
                </li>
              ))}
            </ul>
          )}
          {data.readiness?.ready && (
            <p className="text-sm text-muted-foreground">
              Worker liveness: {data.readiness.runtime_active ? "active" : "not started"}.
            </p>
          )}
        </CardContent>
      </Card>

      <div className="grid gap-4 md:grid-cols-3">
        <Metric label="Available capital" value={money(data.treasury?.available_minor, currency)} />
        <Metric label="Reserved capital" value={money(data.treasury?.reserved_minor, currency)} />
        <Metric label="Net income" value={money(data.accounting?.profit_and_loss.net_income_minor, currency)} />
        <Metric
          label="Urgent event queue"
          value={`${data.event_queue?.high_priority ?? 0} high · ${data.event_queue?.overdue ?? 0} overdue`}
        />
        <Metric
          label="Lifecycle maintenance"
          value={
            data.maintenance
              ? `${secondsSince(data.maintenance.last_checked_at)}s ago`
              : "Not yet run"
          }
        />
        <Metric
          label="Authority integrity"
          value={
            data.authority_integrity
              ? `${data.authority_integrity.status} · ${secondsSince(
                  data.authority_integrity.checked_at,
                )}s ago`
              : "Not yet verified"
          }
        />
        <Metric
          label="Recovery snapshots"
          value={
            data.authority_recovery?.latest_snapshot
              ? `${data.authority_recovery.snapshot_count} retained · ${
                  data.authority_recovery.all_snapshots_valid
                    ? "verified"
                    : "invalid snapshot"
                }`
              : "No known-good snapshot"
          }
        />
        <Metric
          label="Business commitments"
          value={`${data.commitments?.open_count ?? 0} open · ${
            data.commitments?.breached_count ?? 0
          } breached`}
        />
        <Metric
          label="Exact approvals"
          value={`${data.approvals?.pending_count ?? 0} pending execution`}
        />
      </div>

      {(data.commitments?.open.length ?? 0) > 0 && (
        <Card>
          <CardHeader><CardTitle>Open commitments</CardTitle></CardHeader>
          <CardContent className="space-y-2">
            {data.commitments?.open.map((commitment) => (
              <div
                key={commitment.id}
                className="flex items-center justify-between gap-3 text-sm"
              >
                <div>
                  <p className="font-medium">{commitment.title}</p>
                  <p className="text-muted-foreground">
                    {commitment.kind} · due{" "}
                    {new Date(commitment.due_at * 1000).toLocaleString()}
                  </p>
                </div>
                <Badge
                  tone={
                    commitment.status === "breached" ||
                    commitment.overdue
                      ? "destructive"
                      : "secondary"
                  }
                >
                  {commitment.status === "breached"
                    ? "breached"
                    : commitment.overdue
                      ? "overdue"
                      : "active"}
                </Badge>
              </div>
            ))}
          </CardContent>
        </Card>
      )}

      <Card>
        <CardHeader><CardTitle>Objective runtime workers</CardTitle></CardHeader>
        <CardContent className="space-y-2">
          {(data.workers?.length ?? 0) === 0 && (
            <p className="text-sm text-warning">
              No standalone worker heartbeat. Gateway-embedded processing may still be active.
            </p>
          )}
          {data.workers?.map((worker) => (
            <div key={worker.id} className="flex items-center justify-between gap-3 text-sm">
              <div>
                <span className="font-mono">{worker.id}</span>
                <span className="ml-2 text-muted-foreground">
                  last cycle: {worker.last_cycle_status ?? "none"}
                </span>
              </div>
              <Badge tone={worker.healthy ? "secondary" : "destructive"}>
                {worker.effective_status} · {worker.heartbeat_age_seconds}s
              </Badge>
            </div>
          ))}
        </CardContent>
      </Card>

      <Card>
        <CardHeader><CardTitle>Durable event triggers</CardTitle></CardHeader>
        <CardContent className="grid gap-4 md:grid-cols-2">
          <div className="space-y-2">
            <p className="text-sm font-medium">External subscriptions</p>
            {(data.triggers?.subscriptions.length ?? 0) === 0 && (
              <p className="text-xs text-muted-foreground">No external subscriptions.</p>
            )}
            {data.triggers?.subscriptions.map((trigger) => (
              <p key={trigger.id} className="font-mono text-xs">
                {trigger.source_type}.{trigger.event_type} → {trigger.objective_id}
              </p>
            ))}
          </div>
          <div className="space-y-2">
            <p className="text-sm font-medium">Schedules</p>
            {(data.triggers?.schedules.length ?? 0) === 0 && (
              <p className="text-xs text-muted-foreground">No objective schedules.</p>
            )}
            {data.triggers?.schedules.map((trigger) => (
              <p key={trigger.id} className="font-mono text-xs">
                {trigger.event_type} every {trigger.interval_seconds}s
              </p>
            ))}
          </div>
        </CardContent>
      </Card>

      <Card>
        <CardHeader><CardTitle>Measured strategy</CardTitle></CardHeader>
        <CardContent className="grid gap-5 lg:grid-cols-2">
          <div className="space-y-3">
            <p className="text-sm font-medium">Authoritative KPIs</p>
            {(data.strategy_measurement?.metrics.length ?? 0) === 0 && (
              <p className="text-xs text-muted-foreground">No KPI contracts recorded.</p>
            )}
            {data.strategy_measurement?.metrics.map((metric) => (
              <div key={metric.id} className="flex items-start justify-between gap-3 text-sm">
                <div>
                  <p className="font-medium">{metric.name}</p>
                  <p className="text-xs text-muted-foreground">
                    {metric.source_system} · {metric.preferred_direction}
                  </p>
                </div>
                <span className="text-right font-mono text-xs">
                  {metric.latest_observation
                    ? measured(
                        metric.latest_observation.value_scaled,
                        metric.latest_observation.scale,
                        metric.unit,
                      )
                    : "No evidence"}
                </span>
              </div>
            ))}
          </div>
          <div className="space-y-3">
            <p className="text-sm font-medium">Strategy experiments</p>
            {(data.strategy_measurement?.experiments.length ?? 0) === 0 && (
              <p className="text-xs text-muted-foreground">No active or historical experiments.</p>
            )}
            {data.strategy_measurement?.experiments.map((experiment) => (
              <div key={experiment.id} className="rounded border border-border p-3">
                <div className="flex items-start justify-between gap-3">
                  <p className="text-sm font-medium">{experiment.name}</p>
                  <Badge tone={experiment.status === "stopped" ? "destructive" : "secondary"}>
                    {experiment.status}
                  </Badge>
                </div>
                <p className="mt-1 text-xs text-muted-foreground">{experiment.hypothesis}</p>
              </div>
            ))}
          </div>
        </CardContent>
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader><CardTitle>Active objectives</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            {data.objectives.length === 0 && <p className="text-sm text-muted-foreground">No objectives.</p>}
            {data.objectives.slice(0, 20).map((objective) => (
              <div key={objective.id} className="flex items-start justify-between gap-3 border-b border-border pb-3 last:border-0">
                <div>
                  <p className="text-sm font-medium">{objective.desired_outcome}</p>
                  <p className="font-mono text-xs text-muted-foreground">{objective.id}</p>
                </div>
                <Badge tone={objective.status === "blocked" ? "destructive" : "secondary"}>
                  {objective.status}
                </Badge>
              </div>
            ))}
          </CardContent>
        </Card>

        <Card>
          <CardHeader><CardTitle>Exceptions requiring advice</CardTitle></CardHeader>
          <CardContent className="space-y-3">
            {data.exceptions.length === 0 && <p className="text-sm text-muted-foreground">No exceptions.</p>}
            {data.exceptions.map((item) => (
              <div key={`${item.kind}:${item.objective_id}`} className="flex gap-3">
                <AlertTriangle className="h-4 w-4 shrink-0 text-warning" />
                <div>
                  <p className="text-sm font-medium">{item.summary}</p>
                  <p className="text-xs text-muted-foreground">{item.reason}</p>
                </div>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>

      <Card>
        <CardHeader>
          <CardTitle>Advisor intervention queue</CardTitle>
        </CardHeader>
        <CardContent className="space-y-4">
          {(data.interventions?.length ?? 0) === 0 && (
            <p className="text-sm text-muted-foreground">No intervention requested.</p>
          )}
          {data.interventions?.map((item) => (
            <div key={item.id} className="rounded border border-border p-3 space-y-2">
              <p className="text-sm font-medium">{item.summary}</p>
              <p className="text-xs text-muted-foreground">{item.category}</p>
              <pre className="overflow-auto rounded bg-muted p-2 text-xs">
                {JSON.stringify(item.context, null, 2)}
              </pre>
              <div className="flex flex-wrap gap-2">
                {item.options.map((option) => (
                  <Button
                    key={option.id}
                    ghost
                    onClick={() => void resolveIntervention(item.id, option.id)}
                  >
                    {option.label}
                  </Button>
                ))}
              </div>
            </div>
          ))}
        </CardContent>
      </Card>

      <div className="grid gap-4 lg:grid-cols-2">
        <Card>
          <CardHeader><CardTitle>Financial position</CardTitle></CardHeader>
          <CardContent className="grid grid-cols-2 gap-3 text-sm">
            <Line label="Assets" value={money(data.accounting?.balance_sheet.assets_minor, currency)} />
            <Line label="Liabilities" value={money(data.accounting?.balance_sheet.liabilities_minor, currency)} />
            <Line label="Revenue" value={money(data.accounting?.profit_and_loss.revenue_minor, currency)} />
            <Line label="Expenses" value={money(data.accounting?.profit_and_loss.expenses_minor, currency)} />
            <Line label="Tax liability" value={money(data.accounting?.tax_liability_minor, currency)} />
            <Line label="Books balanced" value={data.accounting?.balance_sheet.balanced ? "Yes" : "No"} />
          </CardContent>
        </Card>
        <Card>
          <CardHeader><CardTitle>Organization</CardTitle></CardHeader>
          <CardContent className="space-y-2">
            {data.organization_chart?.map((employee) => (
              <div key={employee.id} style={{ paddingLeft: employee.depth * 16 }} className="text-sm">
                <span className="font-medium">{employee.title}</span>
                <span className="ml-2 text-muted-foreground">{employee.display_name}</span>
                <Badge className="ml-2" tone="secondary">{employee.status}</Badge>
              </div>
            ))}
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

function Metric({ label, value }: { label: string; value: string }) {
  return <Card><CardContent className="py-5"><p className="text-xs text-muted-foreground">{label}</p><p className="mt-1 text-xl font-semibold">{value}</p></CardContent></Card>;
}

function Line({ label, value }: { label: string; value: string }) {
  return <><span className="text-muted-foreground">{label}</span><span className="text-right font-medium">{value}</span></>;
}
