#' @title Visualize structure of the proc.attr.hydfab package
#' @description Run this as new functions are added to proc.attr.hydfab package
#' 
# Changelog/contributions
# 2024-08-07 Originally created, GL

# devtools::install_github("datastorm-open/DependenciesGraphs")
library(DependenciesGraphs)
library(proc.attr.hydfab)
library(htmltools)
dep_pah <- DependenciesGraphs::envirDependencies("package:proc.attr.hydfab")
plt <- plot(dep_pah)
plt_with_title <- htmlwidgets::prependContent(plt, htmltools::tags$h1("proc.attr.hydfab dependency graph"))

