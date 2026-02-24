library(maestro)

schedule <- build_schedule(pipeline_dir = "pipelines")

# show_network(schedule)

status <- run_schedule(schedule)

get_artifacts(schedule)