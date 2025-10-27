`%||%` <- function(x, y) if (is.null(x) || length(x) == 0) y else x

load_yaml <- function(path) {
  if (!file.exists(path)) return(list())
  yaml::read_yaml(path)
}

list_deep_merge <- function(x, y) {
  for (n in names(y)) {
    if (is.list(x[[n]]) && is.list(y[[n]])) x[[n]] <- list_deep_merge(x[[n]], y[[n]]) else x[[n]] <- y[[n]]
  }
  x
}

load_config <- function() {
  base <- load_yaml("config/config.yml")
  loc  <- load_yaml("config/config.local.yml")
  cfg  <- list_deep_merge(base, loc)

  cfg$global$tz <- cfg$global$tz %||% "UTC"
  Sys.setenv(TZ = cfg$global$tz)

  cfg$predictit$api_all <- cfg$predictit$api_all %||% "https://www.predictit.org/api/marketdata/all/"
  dir.create("data/snapshots/predictit", recursive = TRUE, showWarnings = FALSE)
  cfg
}
