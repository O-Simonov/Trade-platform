--
-- PostgreSQL database dump
--

-- Dumped from database version 17.5
-- Dumped by pg_dump version 17.5

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET transaction_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

--
-- Name: _last_ts(regclass, text[]); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public._last_ts(p_table regclass, p_cols text[]) RETURNS timestamp with time zone
    LANGUAGE plpgsql
    AS $$
DECLARE
  c text;
  v timestamptz;
BEGIN
  FOREACH c IN ARRAY p_cols LOOP
    IF EXISTS (
      SELECT 1
      FROM pg_attribute
      WHERE attrelid = p_table
        AND attname = c
        AND NOT attisdropped
    ) THEN
      EXECUTE format('SELECT max(%I)::timestamptz FROM %s', c, p_table) INTO v;
      RETURN v;
    END IF;
  END LOOP;

  RETURN NULL;
END $$;


--
-- Name: candles_set_delta_and_cvd(); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.candles_set_delta_and_cvd() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
DECLARE
  prev_cvd numeric;
BEGIN
  -- delta (???? ?? ?????? - ??????????? ?? taker_*)
  IF NEW.delta_quote IS NULL THEN
    NEW.delta_quote := COALESCE(NEW.taker_buy_quote,0) - COALESCE(NEW.taker_sell_quote,0);
  END IF;

  IF NEW.delta_base IS NULL THEN
    NEW.delta_base := COALESCE(NEW.taker_buy_base,0) - COALESCE(NEW.taker_sell_base,0);
  END IF;

  -- ?????????? cvd
  SELECT c.cvd_quote
    INTO prev_cvd
  FROM candles c
  WHERE c.exchange_id=NEW.exchange_id
    AND c.symbol_id  =NEW.symbol_id
    AND c.interval   =NEW.interval
    AND c.open_time < NEW.open_time
  ORDER BY c.open_time DESC
  LIMIT 1;

  NEW.cvd_quote := COALESCE(prev_cvd, 0) + COALESCE(NEW.delta_quote, 0);
  NEW.updated_at := NOW();

  RETURN NEW;
END;
$$;


--
-- Name: refresh_candles_trades_agg(smallint, bigint, text, timestamp with time zone, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_candles_trades_agg(p_exchange_id smallint, p_symbol_id bigint, p_interval text, p_from timestamp with time zone, p_to timestamp with time zone) RETURNS bigint
    LANGUAGE plpgsql
    AS $$
DECLARE
  bin interval;
  origin timestamptz := timestamptz '1970-01-01 00:00:00+00';
  from_bin timestamptz;
  to_bin   timestamptz;
  base_cvd numeric := 0;
  upserted bigint := 0;
BEGIN
  bin := tf_interval(p_interval);
  IF bin IS NULL THEN
    RAISE EXCEPTION 'Unsupported interval: %', p_interval;
  END IF;

  -- ??????????? ??????? ? ?????
  from_bin := date_bin(bin, p_from, origin);
  to_bin   := date_bin(bin, p_to, origin) + bin;  -- ???????????? "??"

  -- ???? CVD: ????????? cvd_quote ?? from_bin
  SELECT c.cvd_quote
    INTO base_cvd
  FROM candles_trades_agg c
  WHERE c.exchange_id=p_exchange_id
    AND c.symbol_id=p_symbol_id
    AND c.interval=p_interval
    AND c.open_time < from_bin
  ORDER BY c.open_time DESC
  LIMIT 1;

  base_cvd := COALESCE(base_cvd, 0);

  -- ????? ??????? ??????? ???????? (???????/????, ??? ??????? ???????? ??? ???????)
  DELETE FROM candles_trades_agg
  WHERE exchange_id=p_exchange_id
    AND symbol_id=p_symbol_id
    AND interval=p_interval
    AND open_time >= from_bin
    AND open_time <  to_bin;

  WITH agg AS (
    SELECT
      mt.exchange_id,
      mt.symbol_id,
      date_bin(bin, mt.ts, origin) AS open_time,
      SUM(mt.quote_qty) AS quote_volume,
      COUNT(*)::bigint AS trades,
      SUM(CASE WHEN mt.taker_side='BUY'  THEN mt.quote_qty ELSE 0 END) AS taker_buy_quote,
      SUM(CASE WHEN mt.taker_side='SELL' THEN mt.quote_qty ELSE 0 END) AS taker_sell_quote
    FROM market_trades mt
    WHERE mt.exchange_id=p_exchange_id
      AND mt.symbol_id=p_symbol_id
      AND mt.ts >= from_bin
      AND mt.ts <  to_bin
    GROUP BY 1,2,3
  ),
  d AS (
    SELECT
      *,
      (taker_buy_quote - taker_sell_quote) AS delta_quote
    FROM agg
  ),
  r AS (
    SELECT
      exchange_id,
      symbol_id,
      p_interval::text AS interval,
      open_time,
      quote_volume,
      trades,
      taker_buy_quote,
      taker_sell_quote,
      delta_quote,
      (base_cvd + SUM(delta_quote) OVER (
        ORDER BY open_time
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      )) AS cvd_quote
    FROM d
  )
  INSERT INTO candles_trades_agg (
    exchange_id, symbol_id, interval, open_time,
    quote_volume, trades, taker_buy_quote, taker_sell_quote,
    delta_quote, cvd_quote, updated_at
  )
  SELECT
    exchange_id, symbol_id, interval, open_time,
    quote_volume, trades, taker_buy_quote, taker_sell_quote,
    delta_quote, cvd_quote, NOW()
  FROM r;

  GET DIAGNOSTICS upserted = ROW_COUNT;
  RETURN upserted;
END;
$$;


--
-- Name: refresh_candles_trades_agg(integer, integer, text, timestamp with time zone, timestamp with time zone); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.refresh_candles_trades_agg(p_exchange_id integer, p_symbol_id integer, p_interval text, p_from timestamp with time zone, p_to timestamp with time zone) RETURNS bigint
    LANGUAGE sql
    AS $$
  SELECT refresh_candles_trades_agg(
    p_exchange_id::smallint,
    p_symbol_id::bigint,
    p_interval::text,
    p_from,
    p_to
  );
$$;


--
-- Name: tf_interval(text); Type: FUNCTION; Schema: public; Owner: -
--

CREATE FUNCTION public.tf_interval(p text) RETURNS interval
    LANGUAGE sql IMMUTABLE
    AS $$
  SELECT CASE lower(p)
    WHEN '1m'  THEN interval '1 minute'
    WHEN '3m'  THEN interval '3 minutes'
    WHEN '5m'  THEN interval '5 minutes'
    WHEN '15m' THEN interval '15 minutes'
    WHEN '30m' THEN interval '30 minutes'
    WHEN '1h'  THEN interval '1 hour'
    WHEN '2h'  THEN interval '2 hours'
    WHEN '4h'  THEN interval '4 hours'
    WHEN '1d'  THEN interval '1 day'
    ELSE NULL
  END;
$$;


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: account_balance_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.account_balance_snapshots (
    exchange_id smallint NOT NULL,
    account_id smallint NOT NULL,
    ts timestamp with time zone NOT NULL,
    wallet_balance numeric(24,8) NOT NULL,
    equity numeric(24,8) NOT NULL,
    available_balance numeric(24,8) NOT NULL,
    margin_used numeric(24,8) DEFAULT 0,
    unrealized_pnl numeric(24,8) NOT NULL,
    source text DEFAULT 'rest'::text
);


--
-- Name: account_state; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.account_state (
    exchange_id integer NOT NULL,
    account_id integer NOT NULL,
    ts timestamp with time zone NOT NULL,
    wallet_balance numeric,
    equity numeric,
    available_balance numeric,
    unrealized_pnl numeric
);


--
-- Name: accounts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.accounts (
    account_id smallint NOT NULL,
    exchange_id smallint NOT NULL,
    account_name text NOT NULL,
    role text DEFAULT 'MIXED'::text NOT NULL,
    is_active boolean DEFAULT true
);


--
-- Name: accounts_account_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.accounts_account_id_seq
    AS smallint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: accounts_account_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.accounts_account_id_seq OWNED BY public.accounts.account_id;


--
-- Name: candles; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.candles (
    exchange_id smallint NOT NULL,
    symbol_id bigint NOT NULL,
    "interval" text NOT NULL,
    open_time timestamp with time zone NOT NULL,
    open numeric(18,8) NOT NULL,
    high numeric(18,8) NOT NULL,
    low numeric(18,8) NOT NULL,
    close numeric(18,8) NOT NULL,
    volume numeric(28,8) NOT NULL,
    source text DEFAULT 'ws_kline'::text,
    quote_volume numeric(28,12),
    trades bigint,
    taker_buy_base numeric(28,12),
    taker_buy_quote numeric(28,12),
    taker_sell_base numeric(28,12),
    taker_sell_quote numeric(28,12),
    delta_quote numeric(28,12),
    delta_base numeric(28,12),
    cvd_quote numeric(28,12),
    updated_at timestamp with time zone
);


--
-- Name: candles_cvd; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.candles_cvd AS
 SELECT exchange_id,
    symbol_id,
    "interval",
    open_time,
    open,
    high,
    low,
    close,
    volume,
    source,
    quote_volume,
    trades,
    taker_buy_base,
    taker_buy_quote,
    taker_sell_base,
    taker_sell_quote,
    delta_quote,
    delta_base,
    cvd_quote,
    updated_at,
    sum(COALESCE(delta_quote, (0)::numeric)) OVER (PARTITION BY exchange_id, symbol_id, "interval" ORDER BY open_time ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cvd_quote_calc
   FROM public.candles c;


--
-- Name: candles_trades_agg; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.candles_trades_agg (
    exchange_id smallint NOT NULL,
    symbol_id bigint NOT NULL,
    "interval" text NOT NULL,
    open_time timestamp with time zone NOT NULL,
    quote_volume numeric DEFAULT 0 NOT NULL,
    trades bigint DEFAULT 0 NOT NULL,
    taker_buy_quote numeric DEFAULT 0 NOT NULL,
    taker_sell_quote numeric DEFAULT 0 NOT NULL,
    delta_quote numeric DEFAULT 0 NOT NULL,
    cvd_quote numeric DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: exchange_accounts; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.exchange_accounts (
    account_id bigint NOT NULL,
    exchange_id smallint NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);


--
-- Name: exchange_accounts_account_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

ALTER TABLE public.exchange_accounts ALTER COLUMN account_id ADD GENERATED ALWAYS AS IDENTITY (
    SEQUENCE NAME public.exchange_accounts_account_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);


--
-- Name: exchanges; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.exchanges (
    exchange_id smallint NOT NULL,
    name text NOT NULL
);


--
-- Name: exchanges_exchange_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.exchanges_exchange_id_seq
    AS smallint
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: exchanges_exchange_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.exchanges_exchange_id_seq OWNED BY public.exchanges.exchange_id;


--
-- Name: funding; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.funding (
    exchange_id smallint NOT NULL,
    symbol_id bigint NOT NULL,
    funding_time timestamp with time zone NOT NULL,
    funding_rate numeric(10,8) NOT NULL,
    mark_price numeric(18,8),
    source text DEFAULT 'rest'::text
);


--
-- Name: hedge_links; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.hedge_links (
    exchange_id smallint NOT NULL,
    base_account_id smallint NOT NULL,
    hedge_account_id smallint NOT NULL,
    symbol_id bigint NOT NULL,
    base_pos_uid text NOT NULL,
    hedge_pos_uid text NOT NULL,
    hedge_ratio numeric(18,8),
    created_at timestamp with time zone NOT NULL
);


--
-- Name: liquidation_1m; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.liquidation_1m (
    exchange_id integer NOT NULL,
    symbol_id integer NOT NULL,
    bucket_ts timestamp with time zone NOT NULL,
    long_notional numeric(28,12) DEFAULT 0 NOT NULL,
    short_notional numeric(28,12) DEFAULT 0 NOT NULL,
    long_qty numeric(28,12) DEFAULT 0 NOT NULL,
    short_qty numeric(28,12) DEFAULT 0 NOT NULL,
    events integer DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: liq_1m_latest; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.liq_1m_latest AS
 SELECT DISTINCT ON (exchange_id, symbol_id) exchange_id,
    symbol_id,
    bucket_ts,
    long_notional,
    short_notional,
    events
   FROM public.liquidation_1m
  ORDER BY exchange_id, symbol_id, bucket_ts DESC;


--
-- Name: liquidation_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.liquidation_events (
    id bigint NOT NULL,
    exchange_id smallint NOT NULL,
    symbol_id bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    event_ms bigint NOT NULL,
    side text NOT NULL,
    price numeric(28,12) NOT NULL,
    qty numeric(28,12) NOT NULL,
    filled_qty numeric(28,12),
    avg_price numeric(28,12),
    status text,
    order_type text,
    time_in_force text,
    notional numeric(28,12),
    is_long_liq boolean,
    raw_json jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: liquidation_events_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.liquidation_events_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: liquidation_events_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.liquidation_events_id_seq OWNED BY public.liquidation_events.id;


--
-- Name: market_state_5m; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.market_state_5m (
    exchange_id smallint NOT NULL,
    symbol_id bigint NOT NULL,
    open_time timestamp with time zone NOT NULL,
    quote_volume double precision DEFAULT 0 NOT NULL,
    trades bigint DEFAULT 0 NOT NULL,
    taker_buy_quote double precision DEFAULT 0 NOT NULL,
    taker_sell_quote double precision DEFAULT 0 NOT NULL,
    delta_quote double precision DEFAULT 0 NOT NULL,
    cvd_quote double precision DEFAULT 0 NOT NULL,
    oi_ts timestamp with time zone,
    open_interest double precision,
    open_interest_value double precision,
    liq_long_notional double precision DEFAULT 0 NOT NULL,
    liq_short_notional double precision DEFAULT 0 NOT NULL,
    liq_events integer DEFAULT 0 NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: market_trades_5m_delta; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.market_trades_5m_delta AS
 SELECT exchange_id,
    symbol_id,
    open_time,
    quote_volume,
    trades,
    taker_buy_quote,
    taker_sell_quote,
    delta_quote,
    cvd_quote,
    updated_at
   FROM public.candles_trades_agg
  WHERE ("interval" = '5m'::text);


--
-- Name: open_interest; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.open_interest (
    exchange_id smallint NOT NULL,
    symbol_id bigint NOT NULL,
    "interval" text NOT NULL,
    ts timestamp with time zone NOT NULL,
    open_interest numeric(24,8) NOT NULL,
    open_interest_value numeric(24,8) NOT NULL,
    source text DEFAULT 'rest'::text
);


--
-- Name: oi_5m_latest; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.oi_5m_latest AS
 SELECT DISTINCT ON (exchange_id, symbol_id) exchange_id,
    symbol_id,
    ts,
    open_interest,
    open_interest_value
   FROM public.open_interest
  WHERE ("interval" = '5m'::text)
  ORDER BY exchange_id, symbol_id, ts DESC;


--
-- Name: market_state_btc_5m; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.market_state_btc_5m AS
 SELECT t.open_time,
    t.quote_volume,
    t.trades,
    t.taker_buy_quote,
    t.taker_sell_quote,
    t.delta_quote,
    t.cvd_quote,
    oi.ts AS oi_ts,
    oi.open_interest,
    oi.open_interest_value,
    liq.bucket_ts AS liq_ts,
    liq.long_notional,
    liq.short_notional,
    liq.events
   FROM ((public.market_trades_5m_delta t
     LEFT JOIN public.oi_5m_latest oi ON (((oi.exchange_id = t.exchange_id) AND (oi.symbol_id = t.symbol_id))))
     LEFT JOIN public.liq_1m_latest liq ON (((liq.exchange_id = t.exchange_id) AND (liq.symbol_id = t.symbol_id))))
  WHERE ((t.exchange_id = 1) AND (t.symbol_id = 1))
  ORDER BY t.open_time DESC;


--
-- Name: market_trades; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.market_trades (
    exchange_id smallint NOT NULL,
    symbol_id bigint NOT NULL,
    trade_id bigint NOT NULL,
    ts timestamp with time zone NOT NULL,
    price numeric NOT NULL,
    qty numeric NOT NULL,
    quote_qty numeric NOT NULL,
    taker_side text NOT NULL,
    source text,
    raw_json jsonb,
    CONSTRAINT market_trades_taker_side_check CHECK ((taker_side = ANY (ARRAY['BUY'::text, 'SELL'::text])))
);


--
-- Name: order_events; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.order_events (
    exchange_id smallint NOT NULL,
    account_id smallint NOT NULL,
    order_id text NOT NULL,
    symbol_id bigint NOT NULL,
    client_order_id text,
    status text NOT NULL,
    side text,
    type text,
    reduce_only boolean,
    price numeric(18,8),
    qty numeric(18,8),
    filled_qty numeric(18,8),
    source text DEFAULT 'ws_user'::text,
    ts_ms bigint NOT NULL,
    recv_ts timestamp with time zone NOT NULL,
    raw_json text,
    strategy_id text DEFAULT 'unknown'::text NOT NULL,
    pos_uid text
);


--
-- Name: order_fills; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.order_fills (
    exchange_id smallint NOT NULL,
    account_id smallint NOT NULL,
    fill_uid text NOT NULL,
    symbol_id bigint NOT NULL,
    order_id text,
    trade_id text,
    client_order_id text,
    price numeric(18,8),
    qty numeric(18,8),
    realized_pnl numeric(18,8),
    ts timestamp with time zone NOT NULL,
    source text DEFAULT 'ws_user'::text,
    CONSTRAINT order_fills_fill_uid_eq_trade_id CHECK ((fill_uid = trade_id))
);


--
-- Name: orders; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.orders (
    exchange_id smallint NOT NULL,
    account_id smallint NOT NULL,
    order_id text NOT NULL,
    symbol_id bigint NOT NULL,
    strategy_id text DEFAULT 'unknown'::text NOT NULL,
    pos_uid text,
    client_order_id text,
    side text,
    type text,
    reduce_only boolean,
    price numeric(18,8),
    qty numeric(18,8),
    filled_qty numeric(18,8),
    status text,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    source text DEFAULT 'ws_user'::text,
    ts_ms bigint,
    raw_json jsonb,
    avg_price numeric(18,8)
);


--
-- Name: position_ledger; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.position_ledger (
    exchange_id smallint NOT NULL,
    account_id smallint NOT NULL,
    pos_uid text NOT NULL,
    symbol_id bigint NOT NULL,
    strategy_id text DEFAULT 'unknown'::text NOT NULL,
    strategy_name text,
    side text NOT NULL,
    status text DEFAULT 'OPEN'::text NOT NULL,
    opened_at timestamp with time zone NOT NULL,
    closed_at timestamp with time zone,
    entry_price numeric(18,8),
    avg_price numeric(18,8),
    exit_price numeric(18,8),
    qty_opened numeric(18,8) DEFAULT 0 NOT NULL,
    qty_current numeric(18,8) DEFAULT 0 NOT NULL,
    qty_closed numeric(18,8) DEFAULT 0 NOT NULL,
    position_value_usdt numeric(18,8),
    scale_in_count integer DEFAULT 0,
    realized_pnl numeric(18,8) DEFAULT 0,
    fees numeric(18,8) DEFAULT 0,
    updated_at timestamp with time zone NOT NULL,
    source text DEFAULT 'ledger'::text,
    CONSTRAINT position_ledger_side_check CHECK ((side = ANY (ARRAY['LONG'::text, 'SHORT'::text]))),
    CONSTRAINT position_ledger_status_check CHECK ((status = ANY (ARRAY['OPEN'::text, 'CLOSED'::text, 'PARTIAL'::text, 'FLIPPED'::text])))
);


--
-- Name: position_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.position_snapshots (
    exchange_id smallint NOT NULL,
    account_id bigint NOT NULL,
    symbol_id bigint NOT NULL,
    side text NOT NULL,
    qty numeric(24,10) NOT NULL,
    entry_price numeric(24,10) NOT NULL,
    mark_price numeric(24,10) NOT NULL,
    position_value numeric(24,10) DEFAULT 0 NOT NULL,
    unrealized_pnl numeric(24,10) NOT NULL,
    realized_pnl numeric(24,10) NOT NULL,
    fees numeric(24,10) NOT NULL,
    last_ts timestamp with time zone NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    source text,
    avg_price numeric(18,8),
    last_ts_ms bigint
);


--
-- Name: positions; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.positions (
    exchange_id smallint NOT NULL,
    account_id smallint NOT NULL,
    symbol_id bigint NOT NULL,
    strategy_id text DEFAULT 'unknown'::text NOT NULL,
    pos_uid text,
    side text NOT NULL,
    qty numeric(18,8) NOT NULL,
    entry_price numeric(18,8),
    mark_price numeric(18,8),
    unrealized_pnl numeric(18,8),
    updated_at timestamp with time zone NOT NULL,
    source text DEFAULT 'rest'::text,
    avg_price numeric(18,8),
    exit_price numeric(18,8),
    position_value_usdt numeric(18,8),
    leverage integer,
    status text DEFAULT 'OPEN'::text,
    opened_at timestamp with time zone,
    closed_at timestamp with time zone,
    realized_pnl numeric(18,8),
    stop_loss_1 numeric(18,8),
    stop_loss_2 numeric(18,8),
    take_profit_1 numeric(18,8),
    take_profit_2 numeric(18,8),
    take_profit_3 numeric(18,8),
    scale_in_count integer DEFAULT 0,
    strategy_name text,
    fees numeric(18,8) DEFAULT 0,
    last_trade_id text,
    last_ts timestamp with time zone,
    exchange_realized_pnl numeric(18,8) DEFAULT 0,
    CONSTRAINT chk_flat_consistency CHECK ((((side = 'FLAT'::text) AND (qty = (0)::numeric) AND (status = 'CLOSED'::text)) OR ((side = ANY (ARRAY['LONG'::text, 'SHORT'::text])) AND (qty > (0)::numeric) AND (status = 'OPEN'::text)))),
    CONSTRAINT chk_qty_non_negative CHECK ((qty >= (0)::numeric)),
    CONSTRAINT chk_status_valid CHECK ((status = ANY (ARRAY['OPEN'::text, 'CLOSED'::text]))),
    CONSTRAINT chk_symbol_positive CHECK ((symbol_id > 0))
);


--
-- Name: positions_snapshot; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.positions_snapshot AS
 SELECT exchange_id,
    (account_id)::smallint AS account_id,
    symbol_id,
    COALESCE(qty, (0)::numeric(18,8)) AS qty,
    COALESCE(avg_price, (0)::numeric(18,8)) AS avg_price,
    COALESCE(realized_pnl, (0)::numeric(18,8)) AS realized_pnl,
    COALESCE(fees, (0)::numeric(18,8)) AS fees,
    COALESCE(last_ts_ms, (0)::bigint) AS last_ts_ms,
    COALESCE(updated_at, now()) AS updated_at
   FROM public.position_snapshots;


--
-- Name: positions_snapshot_legacy; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.positions_snapshot_legacy AS
 SELECT exchange_id,
    (account_id)::smallint AS account_id,
    symbol_id,
        CASE
            WHEN (side = 'SHORT'::text) THEN (- abs(qty))
            WHEN (side = 'LONG'::text) THEN abs(qty)
            ELSE (0)::numeric
        END AS qty,
    entry_price AS avg_price,
    realized_pnl,
    fees,
    ((EXTRACT(epoch FROM last_ts) * (1000)::numeric))::bigint AS last_ts_ms,
    updated_at
   FROM public.position_snapshots;


--
-- Name: price_snapshots; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.price_snapshots (
    exchange_id smallint NOT NULL,
    symbol_id bigint NOT NULL,
    price numeric(18,8) NOT NULL,
    price_type text NOT NULL,
    ts timestamp with time zone NOT NULL,
    source text DEFAULT 'ws'::text
);


--
-- Name: schema_migrations; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.schema_migrations (
    name text NOT NULL,
    checksum text NOT NULL,
    applied_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: symbol_filters; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.symbol_filters (
    exchange_id smallint NOT NULL,
    symbol_id bigint NOT NULL,
    price_tick numeric(38,18) NOT NULL,
    qty_step numeric(38,18) NOT NULL,
    min_qty numeric(38,18),
    max_qty numeric(38,18),
    min_notional numeric(38,18),
    max_leverage integer,
    margin_type text,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);


--
-- Name: symbols; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.symbols (
    symbol_id bigint NOT NULL,
    exchange_id smallint NOT NULL,
    symbol text NOT NULL,
    is_active boolean DEFAULT true NOT NULL,
    status text,
    last_seen_at timestamp with time zone
);


--
-- Name: symbols_symbol_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.symbols_symbol_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: symbols_symbol_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.symbols_symbol_id_seq OWNED BY public.symbols.symbol_id;


--
-- Name: ticker_24h; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.ticker_24h (
    exchange_id integer NOT NULL,
    symbol_id integer NOT NULL,
    open_time timestamp with time zone NOT NULL,
    close_time timestamp with time zone NOT NULL,
    open_price double precision NOT NULL,
    high_price double precision NOT NULL,
    low_price double precision NOT NULL,
    last_price double precision NOT NULL,
    volume double precision NOT NULL,
    quote_volume double precision NOT NULL,
    price_change double precision NOT NULL,
    price_change_percent double precision NOT NULL,
    weighted_avg_price double precision NOT NULL,
    trades bigint NOT NULL,
    source text NOT NULL
);


--
-- Name: trades; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.trades (
    exchange_id smallint NOT NULL,
    account_id smallint NOT NULL,
    trade_id text NOT NULL,
    order_id text,
    symbol_id bigint NOT NULL,
    strategy_id text DEFAULT 'unknown'::text NOT NULL,
    pos_uid text,
    side text,
    price numeric(18,8),
    qty numeric(18,8),
    fee numeric(18,8),
    fee_asset text,
    realized_pnl numeric(18,8),
    ts timestamp with time zone NOT NULL,
    source text DEFAULT 'ws_user'::text,
    raw_json jsonb,
    quote_qty numeric
);


--
-- Name: v_account_balance_last; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_account_balance_last AS
 SELECT DISTINCT ON (exchange_id, account_id) exchange_id,
    account_id,
    ts,
    wallet_balance,
    equity,
    available_balance,
    margin_used,
    unrealized_pnl,
    source
   FROM public.account_balance_snapshots
  ORDER BY exchange_id, account_id, ts DESC;


--
-- Name: v_account_state_last; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_account_state_last AS
 SELECT DISTINCT ON (exchange_id, account_id) exchange_id,
    account_id,
    ts,
    wallet_balance,
    equity,
    available_balance,
    unrealized_pnl
   FROM public.account_state
  ORDER BY exchange_id, account_id, ts DESC;


--
-- Name: v_candles_trades_agg; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_candles_trades_agg AS
 SELECT exchange_id,
    symbol_id,
    "interval",
    open_time,
    quote_volume,
    trades,
    taker_buy_quote,
    taker_sell_quote,
    delta_quote,
    cvd_quote,
    updated_at,
    open_time AS bucket_ts
   FROM public.candles_trades_agg;


--
-- Name: v_orders_last_event; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_orders_last_event AS
 SELECT DISTINCT ON (order_id) order_id,
    status AS last_event_status,
    filled_qty AS last_event_filled_qty,
    price AS last_event_price,
    ts_ms AS last_event_ts_ms,
    (raw_json)::jsonb AS last_event_raw_json
   FROM public.order_events e
  ORDER BY order_id, ts_ms DESC, filled_qty DESC;


--
-- Name: v_execution_tape; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_execution_tape AS
 SELECT t.ts,
    t.trade_id,
    t.order_id,
    t.symbol_id,
    t.side,
    t.price,
    t.qty,
    t.fee,
    t.fee_asset,
    t.realized_pnl,
    o.strategy_id,
    o.pos_uid,
    COALESCE(le.last_event_status, o.status) AS effective_status,
    le.last_event_status,
    o.status AS orders_status,
    le.last_event_ts_ms
   FROM ((public.trades t
     LEFT JOIN public.orders o USING (order_id))
     LEFT JOIN public.v_orders_last_event le USING (order_id));


--
-- Name: v_funding; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_funding AS
 SELECT exchange_id,
    symbol_id,
    funding_time,
    funding_rate,
    mark_price,
    source,
    funding_time AS ts
   FROM public.funding;


--
-- Name: v_liquidation_1m; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_liquidation_1m AS
 SELECT exchange_id,
    symbol_id,
    bucket_ts,
    long_notional,
    short_notional,
    long_qty,
    short_qty,
    events,
    updated_at,
    bucket_ts AS ts
   FROM public.liquidation_1m;


--
-- Name: v_market_state_5m; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_market_state_5m AS
 SELECT exchange_id,
    symbol_id,
    open_time,
    quote_volume,
    trades,
    taker_buy_quote,
    taker_sell_quote,
    delta_quote,
    cvd_quote,
    oi_ts,
    open_interest,
    open_interest_value,
    liq_long_notional,
    liq_short_notional,
    liq_events,
    updated_at,
    open_time AS bucket_ts
   FROM public.market_state_5m;


--
-- Name: v_market_state_5m_latest; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_market_state_5m_latest AS
 SELECT DISTINCT ON (exchange_id, symbol_id) exchange_id,
    symbol_id,
    open_time,
    quote_volume,
    trades,
    delta_quote,
    cvd_quote,
    open_interest,
    open_interest_value,
    liq_long_notional,
    liq_short_notional,
    liq_events,
    updated_at
   FROM public.market_state_5m
  ORDER BY exchange_id, symbol_id, open_time DESC;


--
-- Name: v_order_events; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_order_events AS
 SELECT exchange_id,
    account_id,
    order_id,
    symbol_id,
    client_order_id,
    status,
    side,
    type,
    reduce_only,
    price,
    qty,
    filled_qty,
    source,
    ts_ms,
    recv_ts,
    raw_json,
    strategy_id,
    pos_uid,
    to_timestamp((((ts_ms)::numeric / 1000.0))::double precision) AS ts
   FROM public.order_events;


--
-- Name: v_order_events_enriched; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_order_events_enriched AS
 SELECT e.exchange_id,
    e.account_id,
    e.order_id,
    e.status,
    e.filled_qty,
    e.price,
    e.ts_ms,
    (e.raw_json)::jsonb AS raw_json,
    o.strategy_id,
    o.pos_uid
   FROM (public.order_events e
     LEFT JOIN public.orders o USING (order_id));


--
-- Name: v_orders_enriched; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_orders_enriched AS
 SELECT o.exchange_id,
    o.account_id,
    o.symbol_id,
    o.order_id,
    o.client_order_id,
    o.side,
    o.type,
    o.reduce_only,
    o.status AS orders_status,
    le.last_event_status,
    COALESCE(le.last_event_status, o.status) AS effective_status,
    COALESCE(le.last_event_filled_qty, o.filled_qty) AS effective_filled_qty,
    o.qty,
    o.price,
    o.avg_price,
    o.filled_qty AS orders_filled_qty,
    le.last_event_filled_qty,
    o.strategy_id,
    o.pos_uid,
    o.source,
    o.ts_ms AS orders_ts_ms,
    le.last_event_ts_ms,
    o.updated_at,
    o.created_at,
    o.raw_json AS orders_raw_json,
    le.last_event_raw_json
   FROM (public.orders o
     LEFT JOIN public.v_orders_last_event le USING (order_id));


--
-- Name: v_positions_last_snapshot; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_positions_last_snapshot AS
 SELECT DISTINCT ON (exchange_id, ((account_id)::smallint), symbol_id) exchange_id,
    (account_id)::smallint AS account_id,
    symbol_id,
    side,
    qty,
    entry_price,
    mark_price,
    position_value,
    unrealized_pnl,
    realized_pnl,
    fees,
    last_ts,
    updated_at,
    source
   FROM public.position_snapshots
  ORDER BY exchange_id, ((account_id)::smallint), symbol_id, last_ts DESC, updated_at DESC;


--
-- Name: v_positions_enriched; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_positions_enriched AS
 SELECT p.exchange_id,
    p.account_id,
    p.symbol_id,
    p.pos_uid,
    p.strategy_id,
    p.strategy_name,
    p.status,
    p.side AS positions_side,
    ls.side AS last_side,
    COALESCE(ls.side, p.side) AS effective_side,
    p.qty AS positions_qty,
    ls.qty AS last_qty,
    COALESCE(ls.qty, p.qty) AS effective_qty,
    p.entry_price,
    COALESCE(ls.entry_price, p.entry_price) AS effective_entry_price,
    COALESCE(ls.mark_price, p.mark_price) AS effective_mark_price,
    COALESCE(ls.unrealized_pnl, p.unrealized_pnl) AS effective_unrealized_pnl,
    p.position_value_usdt,
    ls.position_value AS snapshot_position_value,
    p.realized_pnl,
    p.fees,
    p.last_trade_id,
    p.last_ts,
    p.updated_at
   FROM (public.positions p
     LEFT JOIN public.v_positions_last_snapshot ls ON (((ls.exchange_id = p.exchange_id) AND (ls.account_id = p.account_id) AND (ls.symbol_id = p.symbol_id))));


--
-- Name: v_positions_snapshots_enriched; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_positions_snapshots_enriched AS
 SELECT ps.exchange_id,
    (ps.account_id)::smallint AS account_id,
    ps.symbol_id,
    ps.side,
    ps.qty,
    ps.entry_price,
    ps.mark_price,
    ps.position_value,
    ps.unrealized_pnl,
    ps.realized_pnl,
    ps.fees,
    ps.last_ts,
    ps.updated_at,
    ps.source,
    p.strategy_id,
    p.pos_uid,
    p.status AS position_status
   FROM (public.position_snapshots ps
     LEFT JOIN public.positions p ON (((p.exchange_id = ps.exchange_id) AND (p.account_id = (ps.account_id)::smallint) AND (p.symbol_id = ps.symbol_id))));


--
-- Name: v_ticker_24h; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_ticker_24h AS
 SELECT exchange_id,
    symbol_id,
    open_time,
    close_time,
    open_price,
    high_price,
    low_price,
    last_price,
    volume,
    quote_volume,
    price_change,
    price_change_percent,
    weighted_avg_price,
    trades,
    source,
    close_time AS ts
   FROM public.ticker_24h;


--
-- Name: v_trades_enriched; Type: VIEW; Schema: public; Owner: -
--

CREATE VIEW public.v_trades_enriched AS
 SELECT t.trade_id,
    t.order_id,
    t.exchange_id,
    t.account_id,
    t.symbol_id,
    t.side,
    t.price,
    t.qty,
    t.fee,
    t.fee_asset,
    t.realized_pnl,
    t.ts,
    o.strategy_id,
    o.pos_uid
   FROM (public.trades t
     LEFT JOIN public.orders o USING (order_id));


--
-- Name: accounts account_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounts ALTER COLUMN account_id SET DEFAULT nextval('public.accounts_account_id_seq'::regclass);


--
-- Name: exchanges exchange_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchanges ALTER COLUMN exchange_id SET DEFAULT nextval('public.exchanges_exchange_id_seq'::regclass);


--
-- Name: liquidation_events id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.liquidation_events ALTER COLUMN id SET DEFAULT nextval('public.liquidation_events_id_seq'::regclass);


--
-- Name: symbols symbol_id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.symbols ALTER COLUMN symbol_id SET DEFAULT nextval('public.symbols_symbol_id_seq'::regclass);


--
-- Name: account_balance_snapshots account_balance_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.account_balance_snapshots
    ADD CONSTRAINT account_balance_snapshots_pkey PRIMARY KEY (exchange_id, account_id, ts);


--
-- Name: account_state account_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.account_state
    ADD CONSTRAINT account_state_pkey PRIMARY KEY (exchange_id, account_id, ts);


--
-- Name: accounts accounts_exchange_id_account_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_exchange_id_account_name_key UNIQUE (exchange_id, account_name);


--
-- Name: accounts accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_pkey PRIMARY KEY (account_id);


--
-- Name: candles candles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.candles
    ADD CONSTRAINT candles_pkey PRIMARY KEY (exchange_id, symbol_id, "interval", open_time);


--
-- Name: candles_trades_agg candles_trades_agg_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.candles_trades_agg
    ADD CONSTRAINT candles_trades_agg_pkey PRIMARY KEY (exchange_id, symbol_id, "interval", open_time);


--
-- Name: exchange_accounts exchange_accounts_exchange_id_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange_accounts
    ADD CONSTRAINT exchange_accounts_exchange_id_name_key UNIQUE (exchange_id, name);


--
-- Name: exchange_accounts exchange_accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange_accounts
    ADD CONSTRAINT exchange_accounts_pkey PRIMARY KEY (account_id);


--
-- Name: exchanges exchanges_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchanges
    ADD CONSTRAINT exchanges_name_key UNIQUE (name);


--
-- Name: exchanges exchanges_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchanges
    ADD CONSTRAINT exchanges_pkey PRIMARY KEY (exchange_id);


--
-- Name: funding funding_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.funding
    ADD CONSTRAINT funding_pkey PRIMARY KEY (exchange_id, symbol_id, funding_time);


--
-- Name: hedge_links hedge_links_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.hedge_links
    ADD CONSTRAINT hedge_links_pkey PRIMARY KEY (exchange_id, base_account_id, hedge_account_id, symbol_id, base_pos_uid, hedge_pos_uid);


--
-- Name: liquidation_1m liquidation_1m_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.liquidation_1m
    ADD CONSTRAINT liquidation_1m_pkey PRIMARY KEY (exchange_id, symbol_id, bucket_ts);


--
-- Name: liquidation_events liquidation_events_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.liquidation_events
    ADD CONSTRAINT liquidation_events_pkey PRIMARY KEY (id);


--
-- Name: market_state_5m market_state_5m_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_state_5m
    ADD CONSTRAINT market_state_5m_pkey PRIMARY KEY (exchange_id, symbol_id, open_time);


--
-- Name: market_trades market_trades_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.market_trades
    ADD CONSTRAINT market_trades_pkey PRIMARY KEY (exchange_id, symbol_id, trade_id);


--
-- Name: open_interest open_interest_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.open_interest
    ADD CONSTRAINT open_interest_pkey PRIMARY KEY (exchange_id, symbol_id, "interval", ts);


--
-- Name: order_events order_events_exchange_id_account_id_order_id_ts_ms_status_f_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.order_events
    ADD CONSTRAINT order_events_exchange_id_account_id_order_id_ts_ms_status_f_key UNIQUE (exchange_id, account_id, order_id, ts_ms, status, filled_qty);


--
-- Name: order_fills order_fills_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.order_fills
    ADD CONSTRAINT order_fills_pkey PRIMARY KEY (exchange_id, account_id, fill_uid);


--
-- Name: orders orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_pkey PRIMARY KEY (exchange_id, account_id, order_id);


--
-- Name: position_ledger position_ledger_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.position_ledger
    ADD CONSTRAINT position_ledger_pkey PRIMARY KEY (exchange_id, account_id, pos_uid, opened_at);


--
-- Name: position_snapshots position_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.position_snapshots
    ADD CONSTRAINT position_snapshots_pkey PRIMARY KEY (exchange_id, account_id, symbol_id);


--
-- Name: positions positions_exchange_account_symbol_uniq; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.positions
    ADD CONSTRAINT positions_exchange_account_symbol_uniq UNIQUE (exchange_id, account_id, symbol_id);


--
-- Name: positions positions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.positions
    ADD CONSTRAINT positions_pkey PRIMARY KEY (exchange_id, account_id, symbol_id);


--
-- Name: schema_migrations schema_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.schema_migrations
    ADD CONSTRAINT schema_migrations_pkey PRIMARY KEY (name);


--
-- Name: symbol_filters symbol_filters_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.symbol_filters
    ADD CONSTRAINT symbol_filters_pkey PRIMARY KEY (exchange_id, symbol_id);


--
-- Name: symbols symbols_exchange_id_symbol_key; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.symbols
    ADD CONSTRAINT symbols_exchange_id_symbol_key UNIQUE (exchange_id, symbol);


--
-- Name: symbols symbols_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.symbols
    ADD CONSTRAINT symbols_pkey PRIMARY KEY (symbol_id);


--
-- Name: ticker_24h ticker_24h_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ticker_24h
    ADD CONSTRAINT ticker_24h_pkey PRIMARY KEY (exchange_id, symbol_id, close_time);


--
-- Name: trades trades_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.trades
    ADD CONSTRAINT trades_pkey PRIMARY KEY (exchange_id, account_id, trade_id);


--
-- Name: positions uq_positions_core; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.positions
    ADD CONSTRAINT uq_positions_core UNIQUE (exchange_id, account_id, symbol_id);


--
-- Name: ticker_24h uq_ticker_24h; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.ticker_24h
    ADD CONSTRAINT uq_ticker_24h UNIQUE (exchange_id, symbol_id, close_time);


--
-- Name: position_ledger ux_position_ledger; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.position_ledger
    ADD CONSTRAINT ux_position_ledger UNIQUE (exchange_id, account_id, pos_uid, opened_at);


--
-- Name: idx_account_state_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_account_state_lookup ON public.account_state USING btree (exchange_id, account_id, ts DESC);


--
-- Name: idx_balance_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_balance_lookup ON public.account_balance_snapshots USING btree (exchange_id, account_id, ts DESC);


--
-- Name: idx_candles_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_candles_lookup ON public.candles USING btree (exchange_id, symbol_id, "interval", open_time DESC);


--
-- Name: idx_candles_scan; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_candles_scan ON public.candles USING btree (exchange_id, symbol_id, "interval", open_time);


--
-- Name: idx_candles_symbol_interval; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_candles_symbol_interval ON public.candles USING btree (symbol_id, "interval");


--
-- Name: idx_candles_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_candles_time ON public.candles USING btree (open_time);


--
-- Name: idx_candles_trades_agg_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_candles_trades_agg_lookup ON public.candles_trades_agg USING btree (exchange_id, symbol_id, "interval", open_time DESC);


--
-- Name: idx_candles_wm; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_candles_wm ON public.candles USING btree (exchange_id, "interval", symbol_id, open_time DESC);


--
-- Name: idx_fills_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_fills_lookup ON public.order_fills USING btree (exchange_id, account_id, symbol_id, ts DESC);


--
-- Name: idx_funding_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_funding_lookup ON public.funding USING btree (exchange_id, symbol_id, funding_time DESC);


--
-- Name: idx_market_trades_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_market_trades_ts ON public.market_trades USING btree (exchange_id, symbol_id, ts);


--
-- Name: idx_order_events_event_dedup; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX idx_order_events_event_dedup ON public.order_events USING btree (exchange_id, account_id, order_id, status, filled_qty, ts_ms);


--
-- Name: idx_order_events_oid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_order_events_oid ON public.order_events USING btree (exchange_id, account_id, order_id);


--
-- Name: idx_order_events_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_order_events_ts ON public.order_events USING btree (exchange_id, account_id, ts_ms);


--
-- Name: idx_orders_client_oid; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_orders_client_oid ON public.orders USING btree (exchange_id, account_id, client_order_id);


--
-- Name: idx_orders_new_recent; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_orders_new_recent ON public.orders USING btree (exchange_id, updated_at DESC, symbol_id) WHERE (status = 'NEW'::text);


--
-- Name: idx_orders_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_orders_status ON public.orders USING btree (exchange_id, account_id, status);


--
-- Name: idx_orders_strategy; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_orders_strategy ON public.orders USING btree (strategy_id);


--
-- Name: idx_orders_ts_ms; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_orders_ts_ms ON public.orders USING btree (ts_ms DESC);


--
-- Name: idx_position_ledger_closed_at; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_position_ledger_closed_at ON public.position_ledger USING btree (exchange_id, account_id, closed_at);


--
-- Name: idx_position_ledger_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_position_ledger_status ON public.position_ledger USING btree (exchange_id, account_id, status);


--
-- Name: idx_position_ledger_symbol; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_position_ledger_symbol ON public.position_ledger USING btree (exchange_id, account_id, symbol_id);


--
-- Name: idx_position_snapshots_acc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_position_snapshots_acc ON public.position_snapshots USING btree (exchange_id, account_id);


--
-- Name: idx_position_snapshots_sym; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_position_snapshots_sym ON public.position_snapshots USING btree (exchange_id, symbol_id);


--
-- Name: idx_positions_acc; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_positions_acc ON public.positions USING btree (exchange_id, account_id);


--
-- Name: idx_positions_account; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_positions_account ON public.positions USING btree (exchange_id, account_id);


--
-- Name: idx_positions_last_trade; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_positions_last_trade ON public.positions USING btree (exchange_id, account_id, symbol_id, last_trade_id);


--
-- Name: idx_positions_last_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_positions_last_ts ON public.positions USING btree (last_ts DESC);


--
-- Name: idx_positions_open_symbol; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_positions_open_symbol ON public.positions USING btree (exchange_id, symbol_id) WHERE ((status = 'OPEN'::text) AND (closed_at IS NULL) AND (abs(qty) > (0)::numeric));


--
-- Name: idx_positions_status; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_positions_status ON public.positions USING btree (exchange_id, account_id, status);


--
-- Name: idx_positions_sym; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_positions_sym ON public.positions USING btree (exchange_id, symbol_id);


--
-- Name: idx_snapshots_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_snapshots_lookup ON public.price_snapshots USING btree (exchange_id, symbol_id, ts DESC);


--
-- Name: idx_symbol_filters_updated; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_symbol_filters_updated ON public.symbol_filters USING btree (exchange_id, updated_at DESC);


--
-- Name: idx_symbols_active; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_symbols_active ON public.symbols USING btree (exchange_id, is_active);


--
-- Name: idx_ticker_24h_ex_close_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ticker_24h_ex_close_time ON public.ticker_24h USING btree (exchange_id, close_time);


--
-- Name: idx_ticker_24h_exchange_symbol; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_ticker_24h_exchange_symbol ON public.ticker_24h USING btree (exchange_id, symbol_id);


--
-- Name: idx_trades_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trades_lookup ON public.trades USING btree (exchange_id, account_id, symbol_id, ts DESC);


--
-- Name: idx_trades_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX idx_trades_ts ON public.trades USING btree (ts DESC);


--
-- Name: ix_abs_ex_acc_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_abs_ex_acc_ts ON public.account_balance_snapshots USING btree (exchange_id, account_id, ts DESC);


--
-- Name: ix_account_state_ex_acc_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_account_state_ex_acc_ts ON public.account_state USING btree (exchange_id, account_id, ts DESC);


--
-- Name: ix_candles_5m_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_candles_5m_lookup ON public.candles USING btree (exchange_id, symbol_id, "interval", open_time DESC);


--
-- Name: ix_candles_trades_agg_5m_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_candles_trades_agg_5m_lookup ON public.candles_trades_agg USING btree (exchange_id, symbol_id, open_time DESC) WHERE ("interval" = '5m'::text);


--
-- Name: ix_candles_trades_agg_open_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_candles_trades_agg_open_time ON public.candles_trades_agg USING btree (exchange_id, symbol_id, "interval", open_time);


--
-- Name: ix_liq_1m_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_liq_1m_lookup ON public.liquidation_1m USING btree (exchange_id, symbol_id, bucket_ts DESC);


--
-- Name: ix_liq_1m_lookup2; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_liq_1m_lookup2 ON public.liquidation_1m USING btree (exchange_id, symbol_id, bucket_ts DESC);


--
-- Name: ix_liq_events_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_liq_events_lookup ON public.liquidation_events USING btree (exchange_id, symbol_id, ts DESC);


--
-- Name: ix_liquidation_1m_lookup; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_liquidation_1m_lookup ON public.liquidation_1m USING btree (exchange_id, symbol_id, bucket_ts);


--
-- Name: ix_market_state_5m_ex_open_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_market_state_5m_ex_open_time ON public.market_state_5m USING btree (exchange_id, open_time);


--
-- Name: ix_market_state_5m_symbol_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_market_state_5m_symbol_time ON public.market_state_5m USING btree (exchange_id, symbol_id, open_time DESC);


--
-- Name: ix_open_interest_ex_int_sym_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_open_interest_ex_int_sym_ts ON public.open_interest USING btree (exchange_id, "interval", symbol_id, ts DESC);


--
-- Name: ix_order_events_order_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_order_events_order_ts ON public.order_events USING btree (order_id, ts_ms DESC);


--
-- Name: ix_orders_order_id; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_orders_order_id ON public.orders USING btree (order_id);


--
-- Name: ix_positions_ex_acc_sym; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_positions_ex_acc_sym ON public.positions USING btree (exchange_id, account_id, symbol_id);


--
-- Name: ix_posledger_ex_acc_sym_opened; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_posledger_ex_acc_sym_opened ON public.position_ledger USING btree (exchange_id, account_id, symbol_id, opened_at DESC);


--
-- Name: ix_possnap_ex_acc_sym_lastts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_possnap_ex_acc_sym_lastts ON public.position_snapshots USING btree (exchange_id, account_id, symbol_id, last_ts DESC);


--
-- Name: ix_ticker_24h_ex_close; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_ticker_24h_ex_close ON public.ticker_24h USING btree (exchange_id, close_time DESC);


--
-- Name: ix_ticker_24h_exchange_close_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_ticker_24h_exchange_close_time ON public.ticker_24h USING btree (exchange_id, close_time);


--
-- Name: ix_ticker_24h_symbol_close; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_ticker_24h_symbol_close ON public.ticker_24h USING btree (symbol_id, close_time DESC);


--
-- Name: ix_ticker_24h_symbol_time; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_ticker_24h_symbol_time ON public.ticker_24h USING btree (exchange_id, symbol_id, close_time DESC);


--
-- Name: ix_trades_order_ts; Type: INDEX; Schema: public; Owner: -
--

CREATE INDEX ix_trades_order_ts ON public.trades USING btree (order_id, ts DESC);


--
-- Name: ux_funding_unique; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_funding_unique ON public.funding USING btree (exchange_id, symbol_id, funding_time);


--
-- Name: ux_liq_events_dedupe; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_liq_events_dedupe ON public.liquidation_events USING btree (exchange_id, symbol_id, event_ms, side, price, qty);


--
-- Name: ux_position_ledger_lifecycle; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_position_ledger_lifecycle ON public.position_ledger USING btree (exchange_id, account_id, pos_uid, opened_at);


--
-- Name: ux_position_ledger_uid_open; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_position_ledger_uid_open ON public.position_ledger USING btree (exchange_id, account_id, pos_uid, opened_at);


--
-- Name: ux_posledger_ex_acc_uid_open; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_posledger_ex_acc_uid_open ON public.position_ledger USING btree (exchange_id, account_id, pos_uid, opened_at);


--
-- Name: ux_possnap_ex_acc_sym; Type: INDEX; Schema: public; Owner: -
--

CREATE UNIQUE INDEX ux_possnap_ex_acc_sym ON public.position_snapshots USING btree (exchange_id, account_id, symbol_id);


--
-- Name: candles trg_candles_set_delta_cvd; Type: TRIGGER; Schema: public; Owner: -
--

CREATE TRIGGER trg_candles_set_delta_cvd BEFORE INSERT OR UPDATE OF taker_buy_quote, taker_sell_quote, delta_quote ON public.candles FOR EACH ROW EXECUTE FUNCTION public.candles_set_delta_and_cvd();


--
-- Name: account_balance_snapshots account_balance_snapshots_account_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.account_balance_snapshots
    ADD CONSTRAINT account_balance_snapshots_account_id_fkey FOREIGN KEY (account_id) REFERENCES public.exchange_accounts(account_id) ON DELETE CASCADE;


--
-- Name: account_balance_snapshots account_balance_snapshots_exchange_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.account_balance_snapshots
    ADD CONSTRAINT account_balance_snapshots_exchange_id_fkey FOREIGN KEY (exchange_id) REFERENCES public.exchanges(exchange_id);


--
-- Name: accounts accounts_exchange_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.accounts
    ADD CONSTRAINT accounts_exchange_id_fkey FOREIGN KEY (exchange_id) REFERENCES public.exchanges(exchange_id);


--
-- Name: candles candles_exchange_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.candles
    ADD CONSTRAINT candles_exchange_id_fkey FOREIGN KEY (exchange_id) REFERENCES public.exchanges(exchange_id);


--
-- Name: candles candles_symbol_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.candles
    ADD CONSTRAINT candles_symbol_id_fkey FOREIGN KEY (symbol_id) REFERENCES public.symbols(symbol_id);


--
-- Name: exchange_accounts exchange_accounts_exchange_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.exchange_accounts
    ADD CONSTRAINT exchange_accounts_exchange_id_fkey FOREIGN KEY (exchange_id) REFERENCES public.exchanges(exchange_id);


--
-- Name: open_interest open_interest_exchange_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.open_interest
    ADD CONSTRAINT open_interest_exchange_id_fkey FOREIGN KEY (exchange_id) REFERENCES public.exchanges(exchange_id);


--
-- Name: open_interest open_interest_symbol_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.open_interest
    ADD CONSTRAINT open_interest_symbol_id_fkey FOREIGN KEY (symbol_id) REFERENCES public.symbols(symbol_id);


--
-- Name: symbols symbols_exchange_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.symbols
    ADD CONSTRAINT symbols_exchange_id_fkey FOREIGN KEY (exchange_id) REFERENCES public.exchanges(exchange_id);


--
-- PostgreSQL database dump complete
--

