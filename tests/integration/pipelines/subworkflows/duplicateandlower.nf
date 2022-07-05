include { DUPLICATE } from "../modules/duplicate";
include { LOWER } from "../modules/lower";

workflow DUPLICATE_AND_LOWER {
    take:
        split_file
    
    main:
        DUPLICATE(split_file)
        LOWER(DUPLICATE.out.duplicated_file)
    
    emit:
        lowered_file = LOWER.out.lowered_file
    
}