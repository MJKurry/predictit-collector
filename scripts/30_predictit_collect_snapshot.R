suppressPackageStartupMessages({
  library(httr); library(jsonlite); library(yaml)
})

source("R/config.R")
cfg <- load_config()

api_url <- cfg$predictit$api_all
ua      <- cfg$global$user_agent %||% "pm-study/1.0"
sleep_s <- as.numeric(cfg$global$sleep_seconds_between_requests %||% 0.5)
retries <- as.integer(cfg$global$retries %||% 3)

resp <- httr::RETRY(
  "GET", api_url,
  httr::user_agent(ua),
  httr::timeout(30),
  times = retries,
  pause_min = sleep_s,
  terminate_on = c(400,401,403,404)
)
httr::stop_for_status(resp)
txt <- httr::content(resp, as = "text", encoding = "UTF-8")
if (grepl("^\\s*<", txt)) stop("HTML statt JSON erhalten (Block/Rate-Limit).")

j <- jsonlite::fromJSON(txt, simplifyVector = FALSE)

`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x
get_first_nonnull <- function(...) { xs <- list(...); for (x in xs) if (!is.null(x)) return(x); NULL }

markets_raw <- get_first_nonnull(
  j$Markets, (j$Markets %||% list())$MarketData, j$MarketData,
  j$markets, (j$data %||% list())$markets
)
if (is.null(markets_raw)) stop("'markets' nicht gefunden.")

# in data.frame ohne dplyr/arrow umwandeln
as_market_row <- function(m) {
  m_volume <- m$Volume %||% m$TotalSharesTraded %||% m$totalSharesTraded %||% m$TotalVolume %||% m$totalVolume %||% NA
  data.frame(
    market_id     = suppressWarnings(as.integer(m$ID %||% m$id)),
    market_name   = as.character(m$Name %||% m$name %||% NA_character_),
    market_url    = as.character(m$URL %||% m$url %||% NA_character_),
    market_volume = suppressWarnings(as.numeric(m_volume)),
    stringsAsFactors = FALSE
  )
}

extract_contracts <- function(m, market_id) {
  cc <- m$Contracts %||% m$contracts
  if (is.null(cc)) return(NULL)
  cc <- cc$ContractData %||% cc
  if (!is.list(cc)) return(NULL)
  do.call(rbind, lapply(cc, function(x) {
    c_volume <- x$Volume %||% x$TradeVolume %||% x$TotalSharesTraded %||% x$totalVolume %||% NA
    data.frame(
      market_id          = market_id,
      contract_id        = suppressWarnings(as.integer(x$ID %||% x$id)),
      contract_name      = as.character(x$Name %||% x$name %||% NA_character_),
      last_trade_price   = suppressWarnings(as.numeric(x$LastTradePrice %||% x$lastTradePrice)),
      best_buy_yes_cost  = suppressWarnings(as.numeric(x$BestBuyYesCost %||% x$bestBuyYesCost)),
      best_buy_no_cost   = suppressWarnings(as.numeric(x$BestBuyNoCost %||% x$bestBuyNoCost)),
      best_sell_yes_cost = suppressWarnings(as.numeric(x$BestSellYesCost %||% x$bestSellYesCost)),
      best_sell_no_cost  = suppressWarnings(as.numeric(x$BestSellNoCost %||% x$bestSellNoCost)),
      status             = as.character(x$Status %||% x$status %||% NA_character_),
      contract_volume    = suppressWarnings(as.numeric(c_volume)),
      stringsAsFactors = FALSE
    )
  }))
}

# markets_list herstellen
markets_list <- if (!is.null(markets_raw$MarketData) && is.list(markets_raw$MarketData)) markets_raw$MarketData else markets_raw

# data.frames bauen
mk <- do.call(rbind, lapply(markets_list, as_market_row))
ct <- do.call(rbind, lapply(seq_along(markets_list), function(i) {
  mid <- suppressWarnings(as.integer(mk$market_id[i]))
  extract_contracts(markets_list[[i]], market_id = mid)
}))

# Snapshot-TS (UTC) und join (nur mit Base-R)
snap_ts   <- as.POSIXct(Sys.time(), tz = "UTC")
snap_date <- format(snap_ts, "%Y-%m-%d")
snap_time <- format(snap_ts, "%H%M%S")

# merge für market_name + market_volume an contracts
ct <- merge(ct, mk[, c("market_id","market_name","market_volume")], by = "market_id", all.x = TRUE)

# prob_last normalisieren
prob_last <- ifelse(!is.na(ct$last_trade_price) & ct$last_trade_price > 1 & ct$last_trade_price <= 100,
                    ct$last_trade_price/100, ct$last_trade_price)

out <- data.frame(
  snapshot_ts = rep(format(snap_ts, "%Y-%m-%d %H:%M:%S", tz = "UTC"), nrow(ct)),
  market_id   = ct$market_id,
  market_name = ct$market_name,
  contract_id = ct$contract_id,
  contract_name = ct$contract_name,
  prob_last = prob_last,
  best_buy_yes_cost  = ct$best_buy_yes_cost,
  best_buy_no_cost   = ct$best_buy_no_cost,
  best_sell_yes_cost = ct$best_sell_yes_cost,
  best_sell_no_cost  = ct$best_sell_no_cost,
  status = ct$status,
  contract_volume = ct$contract_volume,
  market_volume   = ct$market_volume,
  stringsAsFactors = FALSE
)

# speichern (CSV.gz)
day_dir <- file.path("data","snapshots","predictit", paste0("date=", snap_date))
if (!dir.exists(day_dir)) dir.create(day_dir, recursive = TRUE)

outfile <- file.path(day_dir, paste0("predictit_", snap_date, "_", snap_time, ".csv.gz"))
con <- gzfile(outfile, "wt")
write.csv(out, con, row.names = FALSE, fileEncoding = "UTF-8")
close(con)

message("Snapshot gespeichert: ", outfile, " (", nrow(out), " Zeilen)")
