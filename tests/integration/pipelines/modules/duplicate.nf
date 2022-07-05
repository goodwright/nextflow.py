process DUPLICATE {
    tag "$file"

    input:
        path file
    
    output:
        path "*", emit: duplicated_file
    

    script:
    """
    #!/usr/bin/env python

    with open("$file") as f:
        lines = f.read().splitlines()

    with open("duplicated_$file", "w") as f:
        f.write("\\n".join(lines * $params.count) + "\\n")
    """

}