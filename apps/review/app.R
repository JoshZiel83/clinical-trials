# Phase 6F — HITL review app (read-only DuckDB).
#
# One tab per domain (condition / drug / sponsor). Candidates from
# ref.mapping_candidates are shown with filters on status/score/study_count/
# source; expandable row shows rationale + tool_trace for agent proposals.
# Batch approve/reject actions write a Parquet decision log to
# `../../data/reviews/decisions_YYYY-MM-DD_HHMMSS.parquet`. The Python-side
# `run_hitl_sync.py` imports that log and promotes approvals.
#
# IMPORTANT: never holds a write lock on the DuckDB — opens read-only so the
# pipeline can run concurrently.

suppressPackageStartupMessages({
  library(shiny)
  library(DT)
  library(duckdb)
  library(arrow)
  library(dplyr)
  library(jsonlite)
})

# ---------- Paths ----------------------------------------------------------
# When Shiny launches the app, getwd() is the app folder (`apps/review/`),
# so the project root is two levels up. Allow override via env var.
APP_DIR <- normalizePath(getwd())
PROJECT_ROOT <- normalizePath(
  Sys.getenv("CLINICAL_TRIALS_ROOT", file.path(APP_DIR, "..", "..")),
  mustWork = FALSE
)
DB_PATH <- file.path(PROJECT_ROOT, "data", "clinical_trials.duckdb")
REVIEWS_DIR <- file.path(PROJECT_ROOT, "data", "reviews")
if (!file.exists(DB_PATH)) {
  stop(sprintf(
    "DuckDB not found at %s. Either launch with `shiny::runApp(\"apps/review\")` from the project root, or set CLINICAL_TRIALS_ROOT.",
    DB_PATH
  ))
}
dir.create(REVIEWS_DIR, showWarnings = FALSE, recursive = TRUE)

DOMAINS <- c("condition", "drug", "sponsor")

# ---------- Data access (read-only) ----------------------------------------
load_candidates <- function(domain) {
  con <- dbConnect(duckdb(), dbdir = DB_PATH, read_only = TRUE)
  on.exit(dbDisconnect(con, shutdown = TRUE))
  dbGetQuery(con, sprintf("
    SELECT domain, source_value, canonical_term, canonical_id,
           score, study_count, source, rationale, tool_trace,
           status, created_at
    FROM ref.mapping_candidates
    WHERE domain = '%s'
    ORDER BY study_count DESC, score DESC
  ", domain))
}

pretty_trace <- function(tool_trace_json) {
  if (is.null(tool_trace_json) || is.na(tool_trace_json) || tool_trace_json == "") {
    return("<no tool trace>")
  }
  out <- tryCatch(fromJSON(tool_trace_json, simplifyVector = FALSE),
                  error = function(e) NULL)
  if (is.null(out)) return(tool_trace_json)
  toJSON(out, auto_unbox = TRUE, pretty = TRUE)
}

# ---------- UI --------------------------------------------------------------
domain_tab <- function(dom) {
  tabPanel(
    title = tools::toTitleCase(dom),
    value = dom,
    fluidRow(
      column(3, selectInput(paste0(dom, "_status"), "Status",
                            c("pending", "approved", "rejected", "all"),
                            selected = "pending")),
      column(3, selectInput(paste0(dom, "_source"), "Source",
                            c("all", "fuzzy", "quickumls", "agent", "co-occurrence"),
                            selected = "all")),
      column(3, sliderInput(paste0(dom, "_min_score"), "Min score",
                            min = 0, max = 100, value = 0, step = 1)),
      column(3, numericInput(paste0(dom, "_min_studies"), "Min studies",
                             value = 1, min = 0, step = 1))
    ),
    hr(),
    fluidRow(
      column(12,
        actionButton(paste0(dom, "_approve"), "Approve selected", class = "btn-success"),
        actionButton(paste0(dom, "_reject"), "Reject selected", class = "btn-danger"),
        span(style = "margin-left: 20px;", textOutput(paste0(dom, "_count"), inline = TRUE)),
        span(style = "margin-left: 20px;", textOutput(paste0(dom, "_pending_pool"), inline = TRUE))
      )
    ),
    br(),
    DTOutput(paste0(dom, "_table")),
    br(),
    verbatimTextOutput(paste0(dom, "_detail"))
  )
}

ui <- fluidPage(
  titlePanel("Clinical Trials — HITL Mapping Review"),
  tags$p(tags$em(
    "Read-only view of ref.mapping_candidates. Approve/reject writes to data/reviews/*.parquet; ",
    tags$code("python run_hitl_sync.py"), " applies the decisions."
  )),
  div(style = "padding: 8px; background: #f4f6fa; border-left: 4px solid #4a90e2; margin-bottom: 10px;",
      strong("Session activity: "),
      textOutput("session_summary", inline = TRUE)),
  do.call(tabsetPanel, c(list(id = "domain_tab"), lapply(DOMAINS, domain_tab))),
  hr(),
  div(id = "reviewer_panel",
      fluidRow(
        column(4, textInput("reviewer", "Reviewer", value = Sys.getenv("USER", "unknown"))),
        column(8, textOutput("last_writeout"))
      ))
)

# ---------- Server ----------------------------------------------------------
server <- function(input, output, session) {

  # In-session decisions overlaid on the DB-loaded status. Keyed by
  # paste(source_value, canonical_term, source, sep="\037"). Value is
  # "approved" / "rejected". Initialized per domain.
  rv <- reactiveValues(
    last_writeout = "No decisions written this session.",
    decisions = setNames(lapply(DOMAINS, function(x) character()), DOMAINS)
  )

  output$last_writeout <- renderText(rv$last_writeout)

  output$session_summary <- renderText({
    parts <- vapply(DOMAINS, function(d) {
      decs <- rv$decisions[[d]]
      n_app <- sum(decs == "approved")
      n_rej <- sum(decs == "rejected")
      if (n_app + n_rej == 0) return(NA_character_)
      sprintf("%s: %d approved / %d rejected", tools::toTitleCase(d), n_app, n_rej)
    }, character(1))
    parts <- parts[!is.na(parts)]
    if (length(parts) == 0) {
      "no decisions yet — select rows and click Approve/Reject."
    } else {
      paste0(paste(parts, collapse = "  •  "),
             "  •  Run `python run_hitl_sync.py` to apply.")
    }
  })

  decision_key <- function(row) {
    paste(row$source_value, row$canonical_term, row$source, sep = "\037")
  }

  for (dom in DOMAINS) {
    local({
      d <- dom

      # Re-loads on every change to rv$decisions[[d]] so newly-decided rows
      # show their effective_status immediately.
      candidates_with_overlay <- reactive({
        df <- load_candidates(d)
        if (nrow(df) == 0) {
          df$effective_status <- character(0)
          return(df)
        }
        keys <- paste(df$source_value, df$canonical_term, df$source, sep = "\037")
        decs <- rv$decisions[[d]]
        df$effective_status <- df$status
        hit <- keys %in% names(decs)
        df$effective_status[hit] <- paste0(decs[keys[hit]], "* (this session)")
        df
      })

      filtered <- reactive({
        df <- candidates_with_overlay()
        status_f <- input[[paste0(d, "_status")]]
        source_f <- input[[paste0(d, "_source")]]
        min_score <- input[[paste0(d, "_min_score")]]
        min_studies <- input[[paste0(d, "_min_studies")]]

        if (!is.null(status_f) && status_f != "all") {
          df <- df[startsWith(df$effective_status, status_f), ]
        }
        if (!is.null(source_f) && source_f != "all") df <- df[df$source == source_f, ]
        if (!is.null(min_score)) df <- df[df$score >= min_score, ]
        if (!is.null(min_studies)) df <- df[df$study_count >= min_studies, ]
        df
      })

      output[[paste0(d, "_table")]] <- renderDT({
        df <- filtered()
        display <- df[, c("source_value", "canonical_term", "canonical_id",
                          "score", "study_count", "source", "effective_status")]
        names(display)[7] <- "status"
        datatable(
          display,
          selection = "multiple",
          options = list(pageLength = 20, order = list(list(4, "desc"))),
          rownames = FALSE,
        ) |>
          formatRound("score", digits = 2) |>
          formatStyle(
            "status",
            target = "row",
            backgroundColor = styleEqual(
              c("approved* (this session)", "rejected* (this session)"),
              c("#d4edda", "#f8d7da")
            )
          )
      })

      output[[paste0(d, "_count")]] <- renderText({
        df <- filtered()
        sprintf("Showing %d candidate(s)", nrow(df))
      })

      output[[paste0(d, "_pending_pool")]] <- renderText({
        df <- candidates_with_overlay()
        n_pending <- sum(df$effective_status == "pending")
        sprintf("(queue: %d pending for %s, not counting this-session decisions)",
                n_pending, d)
      })

      output[[paste0(d, "_detail")]] <- renderPrint({
        df <- filtered()
        sel <- input[[paste0(d, "_table_rows_selected")]]
        if (length(sel) == 0) {
          cat("Select a row to see its rationale + tool trace.")
          return()
        }
        row <- df[sel[1], ]
        cat("=== ", row$source_value, " → ", row$canonical_term, " ===\n", sep = "")
        cat("source: ", row$source, "    score: ", round(row$score, 2),
            "    studies: ", row$study_count, "\n", sep = "")
        rationale_ok <- !is.null(row$rationale) && !is.na(row$rationale) &&
                        nzchar(as.character(row$rationale))
        if (rationale_ok) {
          cat("\nRationale:\n", row$rationale, "\n", sep = "")
        }
        trace_ok <- !is.null(row$tool_trace) && !is.na(row$tool_trace) &&
                    nzchar(as.character(row$tool_trace))
        if (trace_ok) {
          cat("\nTool trace:\n", pretty_trace(row$tool_trace), "\n", sep = "")
        }
      })

      record_decision <- function(decision) {
        df <- filtered()
        sel <- input[[paste0(d, "_table_rows_selected")]]
        if (length(sel) == 0) {
          showNotification("Select at least one row first.",
                           type = "warning", duration = 4)
          return()
        }
        rows <- df[sel, c("domain", "source_value", "canonical_term",
                          "canonical_id", "source")]
        rows$decision <- decision
        rows$reviewer <- input$reviewer
        rows$decided_at <- format(Sys.time(), "%Y-%m-%dT%H:%M:%SZ", tz = "UTC")

        stamp <- format(Sys.time(), "%Y-%m-%d_%H%M%S")
        path <- file.path(REVIEWS_DIR,
                          sprintf("decisions_%s_%s.parquet", stamp, d))
        write_parquet(rows, path)

        # Update in-session overlay so the table reflects the decision NOW.
        new_keys <- paste(rows$source_value, rows$canonical_term,
                          rows$source, sep = "\037")
        cur <- rv$decisions[[d]]
        cur[new_keys] <- decision
        rv$decisions[[d]] <- cur

        rv$last_writeout <- sprintf(
          "Wrote %d decisions (%s) to %s. Run `python run_hitl_sync.py` to apply.",
          nrow(rows), decision, basename(path)
        )
        showNotification(
          sprintf("✓ Recorded %d %s decision(s) → %s",
                  nrow(rows), decision, basename(path)),
          type = if (decision == "approved") "message" else "warning",
          duration = 6
        )
      }

      observeEvent(input[[paste0(d, "_approve")]], {
        record_decision("approved")
      })
      observeEvent(input[[paste0(d, "_reject")]], {
        record_decision("rejected")
      })
    })
  }
}

# Small null-coalesce helper, since sys.frame is NULL outside sourced scripts
`%||%` <- function(a, b) if (!is.null(a)) a else b

shinyApp(ui = ui, server = server)
