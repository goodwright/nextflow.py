process DUPLICATE_LINE {
    input:
        path file
        val count

    output:
        path "duplicated.txt", emit: combined

    script:
    """
    #!/usr/bin/env python
    if "$params.wait".isnumeric():
        import time
        time.sleep(int("$params.wait"))
    with open("$file") as f:
        lines = [f.read().splitlines()[0]] * $count
    with open("duplicated.txt", "w") as f:
        f.write("\\n".join(lines))
    """
}