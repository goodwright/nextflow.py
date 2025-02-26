process LOWER {
    tag "$file"

    input:
        path file
    
    output:
        path "*", emit: lowered_file

    script:
    """
    #!/usr/bin/env python

    with open("$file") as f:
        text = f.read()
    
    if "$params.flag" == "xxx":
        raise Exception("Error")
    
    with open("lowered_$file", "w") as f:
        f.write(text.lower())
    """

}