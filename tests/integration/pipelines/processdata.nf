nextflow.enable.dsl=2

include { COMBINE_LINES } from "./modules/combine_lines";
include { DUPLICATE_LINE } from "./modules/duplicate_line";

workflow PROCESS_DATA {
    take:
        data
    
    main:
        COMBINE_LINES(data)
        DUPLICATE_LINE(COMBINE_LINES.out, params.count)
}