import { Badge, availabilityTone, freshnessTone } from "@/components/ui/badge";
import { formatLocal, formatUtc } from "@/lib/format";

export function MetaRow({
  asOf,
  receivedAt,
  freshness,
  availability,
  coverage,
  uncertainty,
  sourceMode,
}: {
  asOf?: string | null;
  receivedAt?: string | null;
  freshness?: string | null;
  availability?: string | null;
  coverage?: string | null;
  uncertainty?: string | null;
  sourceMode?: string | null;
}) {
  return (
    <div className="flex flex-wrap items-start gap-2 text-xs">
      {freshness ? <Badge tone={freshnessTone(freshness)}>freshness {freshness}</Badge> : null}
      {availability ? (
        <Badge tone={availabilityTone(availability)}>availability {availability}</Badge>
      ) : null}
      {uncertainty ? <Badge tone="warning">uncertainty {uncertainty}</Badge> : null}
      {sourceMode ? <Badge tone="neutral">{sourceMode}</Badge> : null}
      <span className="text-zinc-600 dark:text-zinc-400">
        asOf UTC {formatUtc(asOf)} · local {formatLocal(asOf)}
      </span>
      <span className="text-zinc-600 dark:text-zinc-400">
        receivedAt UTC {formatUtc(receivedAt)} · local {formatLocal(receivedAt)}
      </span>
      {coverage ? (
        <span className="w-full text-zinc-500 dark:text-zinc-400">coverage: {coverage}</span>
      ) : null}
    </div>
  );
}
