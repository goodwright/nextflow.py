process APPEND {
    tag "$file"

    input:
        path file
        path suffix
    
    output:
        path "*", emit: suffix_file

    script:
    """
    cat $file $suffix > suffix_$file
    """

}