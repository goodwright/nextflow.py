nextflow.enable.dsl=2

include { SPLIT_FILE } from "./modules/split_file";
include { PROCESS_DATA } from "./processdata";

workflow {
    SPLIT_FILE(params.file)
    PROCESS_DATA(SPLIT_FILE.out.split_files.flatten())
}