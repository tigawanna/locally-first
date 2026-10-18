import { SiteIcon } from "@/components/icon/SiteIcon";
import { useReportSiteError } from "@/lib/tanstack/router/use-report-site-error";
import { Link, type ErrorComponentProps } from "@tanstack/react-router";
import { Check, Copy } from "lucide-react";
import { useState } from "react";

function toError(error: unknown) {
  if (error instanceof Error) {
    return error;
  }

  if (typeof error === "string") {
    return new Error(error);
  }

  return new Error("Unknown error");
}

export function RouterErrorComponent({ error }: ErrorComponentProps) {
  const resolvedError = toError(error);
  useReportSiteError(resolvedError);

  return (
    <div
      data-test="router-error"
      className="landing-void-surface relative flex min-h-screen flex-col"
    >
      <div className="landing-void-glow-center pointer-events-none absolute inset-0" />

      <div className="relative z-10 flex flex-1 flex-col items-center justify-center px-6 py-20">
        <RouterErrorProductionContent />
      </div>

      {import.meta.env.DEV ? <RouterErrorDevelopmentPanel error={resolvedError} /> : null}
    </div>
  );
}

export function RouterErrorProduction() {
  return (
    <div
      data-test="router-error-production"
      className="landing-void-surface relative flex min-h-screen flex-col items-center justify-center px-6 py-20"
    >
      <div className="landing-void-glow-center pointer-events-none absolute inset-0" />
      <div className="relative z-10">
        <RouterErrorProductionContent />
      </div>
    </div>
  );
}

function RouterErrorProductionContent() {
  return (
    <div className="flex max-w-md flex-col items-center text-center">
      <SiteIcon size={88} className="text-landing-sage/35" aria-hidden />

      <p className="mt-8 text-[11px] tracking-[0.32em] text-landing-sage/50 uppercase">
        Unexpected hiccup
      </p>

      <h1 className="mt-4 font-serif text-4xl leading-tight tracking-[-0.03em] md:text-5xl">
        Something went sideways
      </h1>

      <p className="mt-5 text-base leading-7 text-landing-sage/65">
        This page hit a snag on our end. Give it another moment, or head back home while we sort
        things out.
      </p>

      <svg
        viewBox="0 0 400 24"
        fill="none"
        xmlns="http://www.w3.org/2000/svg"
        className="mt-10 h-5 w-full max-w-xs text-landing-sage/25"
        aria-hidden="true"
      >
        <path
          d="M0 14C60 4 120 22 200 12C280 2 340 20 400 10"
          stroke="currentColor"
          strokeWidth="1"
        />
      </svg>

      <div className="mt-8 flex flex-wrap items-center justify-center gap-3">
        <Link
          to="/"
          data-test="router-error-home"
          className="rounded-full border border-landing-sage/20 bg-landing-sage/8 px-6 py-2.5 text-sm text-landing-sage transition-colors hover:bg-landing-sage/14"
        >
          Back home
        </Link>
        <button
          type="button"
          data-test="router-error-retry"
          onClick={() => window.location.reload()}
          className="rounded-full border border-landing-sage/10 px-6 py-2.5 text-sm text-landing-sage/70 transition-colors hover:border-landing-sage/20 hover:text-landing-sage"
        >
          Try again
        </button>
      </div>
    </div>
  );
}

function RouterErrorDevelopmentPanel({ error }: { error: Error }) {
  const [copied, setCopied] = useState(false);

  const copyStackTrace = async () => {
    const text = error.stack ?? `${error.name}: ${error.message}`;
    await navigator.clipboard.writeText(text);
    setCopied(true);
    setTimeout(() => setCopied(false), 2000);
  };

  return (
    <div
      data-test="router-error-development"
      className="relative z-10 border-t border-landing-sage/15 bg-landing-panel/95 px-4 py-5 backdrop-blur-sm"
    >
      <div className="mx-auto w-full max-w-3xl">
        <div className="flex items-center justify-between gap-4">
          <p className="text-[11px] tracking-[0.28em] text-landing-sage/45 uppercase">
            Dev details
          </p>
          <button
            type="button"
            onClick={copyStackTrace}
            data-test="router-error-copy"
            className="inline-flex items-center gap-1.5 rounded-full border border-landing-sage/15 px-3 py-1.5 text-xs text-landing-sage/70 transition-colors hover:border-landing-sage/25 hover:text-landing-sage"
          >
            {copied ? <Check className="size-3.5" /> : <Copy className="size-3.5" />}
            {copied ? "Copied" : "Copy"}
          </button>
        </div>

        <p className="mt-3 font-mono text-sm text-landing-sage">{error.name}</p>
        <p className="mt-1 font-mono text-sm leading-6 text-landing-sage/70">{error.message}</p>

        {error.stack ? (
          <details className="mt-4 group">
            <summary className="cursor-pointer text-sm text-landing-sage/60 transition-colors hover:text-landing-sage">
              Stack trace
            </summary>
            <pre className="mt-3 max-h-64 overflow-auto rounded-lg border border-landing-sage/10 bg-black/25 p-3 font-mono text-xs leading-5 text-landing-sage/75">
              {error.stack}
            </pre>
          </details>
        ) : null}
      </div>
    </div>
  );
}
