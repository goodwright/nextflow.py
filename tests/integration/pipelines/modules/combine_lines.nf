process COMBINE_LINES {
    input:
        path file

    output:
        path "combined.txt", emit: combined

    script:
    """
    #!/usr/bin/env python
    with open("$file") as f1:
        with open("combined.txt", "w") as f2:
            f2.write(" ".join(f1.read().splitlines()))
    """
}