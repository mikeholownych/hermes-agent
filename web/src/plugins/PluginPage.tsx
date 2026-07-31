import { useSyncExternalStore } from "react";
import { Spinner } from "@nous-research/ui/ui/components/spinner";
import {
  getPluginComponent,
  getPluginLoadError,
  onPluginRegistered,
} from "./registry";
import { useI18n } from "@/i18n";
import { cn } from "@/lib/utils";
import type { Translations } from "@/i18n/types";

/** Renders a plugin tab once its bundle has called `register()`. */
export function PluginPage({ name }: { name: string }) {
  const { t } = useI18n();
  // Subscribe in render (via useSyncExternalStore) so we never miss
  // `register()` if the script loads before a useEffect would run.
  const Component = useSyncExternalStore(
    (onChange) => onPluginRegistered(onChange),
    () => getPluginComponent(name) ?? null,
    () => null,
  );
  const loadError = useSyncExternalStore(
    (onChange) => onPluginRegistered(onChange),
    () => getPluginLoadError(name) ?? null,
    () => null,
  );

  if (Component) {
    // Not actually created during render: getPluginComponent reads a stable
    // reference from registry.ts's module-level Map, populated once by the
    // plugin's own register() call. useSyncExternalStore's getSnapshot just
    // reads the current registered value each render — the linter can't see
    // through that indirection to know it's stable, not freshly constructed.
    // eslint-disable-next-line react-hooks/static-components
    return <Component />;
  }

  if (loadError) {
    const message = formatPluginError(loadError, t);
    return (
      <div
        className={cn(
          "max-w-lg p-4",
          "font-mondwest text-sm tracking-[0.08em] text-text-secondary",
        )}
        role="alert"
      >
        {message}
      </div>
    );
  }

  return (
    <div
      className={cn(
        "flex items-center gap-2 p-4",
        "font-mondwest text-sm tracking-[0.1em] text-text-tertiary",
      )}
    >
      <Spinner className="shrink-0" />
      <span>{t.common.loading}</span>
    </div>
  );
}

function formatPluginError(code: string, t: Translations): string {
  if (code === "LOAD_FAILED") return t.common.pluginLoadFailed;
  if (code === "NO_REGISTER") return t.common.pluginNotRegistered;
  return code;
}
