process SPLIT_FILE {
    input:
        path file

    output:
        path("*.dat"), emit: split_files

    script:
    """
    #!/usr/bin/env python
    with open("$file") as f:
        lines = f.read().splitlines()

    if "$params.wait".isnumeric():
        import time
        time.sleep(int("$params.wait"))
    splits = {}
    for line in lines:
        first = line.split()[0]
        if first not in splits: splits[first] = []
        splits[first].append(line[len(first):].lstrip())
    for name, lines in splits.items():
        with open(f"{name}.dat", "w") as f:
            f.write("\\n".join(lines))
    """
}