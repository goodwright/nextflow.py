include { DUPLICATE_AND_LOWER } from "./duplicateandlower";
include { APPEND } from "../modules/append";

workflow PROCESS_DATA {
    take:
        split_file

    main:
        DUPLICATE_AND_LOWER(split_file)
        APPEND(DUPLICATE_AND_LOWER.out.lowered_file, file(params.suffix))
    
    emit:
        suffix_file = APPEND.out.suffix_file

}