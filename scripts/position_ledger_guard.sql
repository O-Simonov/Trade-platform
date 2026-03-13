-- DB-level guard for position_ledger timestamps
CREATE OR REPLACE FUNCTION public.trg_position_ledger_guard_closed_at()
RETURNS trigger
LANGUAGE plpgsql
AS $$
BEGIN
    IF NEW.opened_at IS NOT NULL AND NEW.closed_at IS NOT NULL AND NEW.closed_at < NEW.opened_at THEN
        IF NEW.updated_at IS NOT NULL AND NEW.updated_at >= NEW.opened_at THEN
            NEW.closed_at := NEW.updated_at;
        ELSE
            NEW.closed_at := NEW.opened_at;
        END IF;
    END IF;

    IF NEW.updated_at IS NOT NULL AND NEW.opened_at IS NOT NULL AND NEW.updated_at < NEW.opened_at THEN
        NEW.updated_at := NEW.opened_at;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_position_ledger_guard_closed_at ON public.position_ledger;

CREATE TRIGGER trg_position_ledger_guard_closed_at
BEFORE INSERT OR UPDATE ON public.position_ledger
FOR EACH ROW
EXECUTE FUNCTION public.trg_position_ledger_guard_closed_at();

UPDATE public.position_ledger
   SET closed_at = CASE
       WHEN updated_at IS NOT NULL AND opened_at IS NOT NULL AND updated_at >= opened_at THEN updated_at
       WHEN opened_at IS NOT NULL THEN opened_at
       ELSE closed_at
   END
 WHERE opened_at IS NOT NULL
   AND closed_at IS NOT NULL
   AND closed_at < opened_at;

ALTER TABLE public.position_ledger
DROP CONSTRAINT IF EXISTS chk_position_ledger_closed_at_not_before_opened_at;

ALTER TABLE public.position_ledger
ADD CONSTRAINT chk_position_ledger_closed_at_not_before_opened_at
CHECK (closed_at IS NULL OR opened_at IS NULL OR closed_at >= opened_at) NOT VALID;

ALTER TABLE public.position_ledger
VALIDATE CONSTRAINT chk_position_ledger_closed_at_not_before_opened_at;

-- Periodic health check
SELECT
    COUNT(*) FILTER (WHERE opened_at IS NOT NULL AND closed_at IS NOT NULL AND closed_at < opened_at) AS broken_closed_at,
    COUNT(*) FILTER (WHERE opened_at IS NOT NULL AND updated_at IS NOT NULL AND updated_at < opened_at) AS broken_updated_at,
    COUNT(*) FILTER (WHERE closed_at IS NOT NULL AND updated_at IS NOT NULL AND closed_at > updated_at) AS broken_closed_gt_updated
FROM public.position_ledger;
