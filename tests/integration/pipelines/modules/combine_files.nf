process COMBINE_FILES {
    input:
        path file

    output:
        path "combined.txt", emit: combined

    script:
    """
    #!/usr/bin/env python
    if "$params.wait".isnumeric():
        import time
        time.sleep(int("$params.wait"))
    files = "$file".split()
    text = ""
    for file in files:
        with open(file) as f:
            text += f.read()
    with open("combined.txt", "w") as f:
        f.write(text)
    """
}