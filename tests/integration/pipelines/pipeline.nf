nextflow.enable.dsl=2

include { PROCESS_DATA } from "./subworkflows/processdata";
include { SPLIT_FILE } from "./modules/split_file";
include { COMBINE_FILES } from "./modules/combine_files";

workflow {
    SPLIT_FILE(file(params.input))
    PROCESS_DATA(SPLIT_FILE.out.split_files.flatten())
    JOIN(PROCESS_DATA.out.suffix_file.collect())
}

workflow JOIN {
    take:
        files

    main:
        COMBINE_FILES(files)
}