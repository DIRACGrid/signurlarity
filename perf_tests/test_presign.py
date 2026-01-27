# import json
# import time
# import sys
# from pathlib import Path


# def something(duration=0.000001):
#     """
#     Function that needs some serious benchmarking.
#     """
#     time.sleep(duration)
#     # You may return anything you want, like the result of a computation
#     return 123


# def test_my_stuff(perf_test_dir):
#     # benchmark something
#     py_vers = sys.version_info
#     result_file: Path = perf_test_dir / Path(f"{py_vers.major}.{py_vers.minor}.json")
#     result_file.write_text(json.dumps({1: 2}, indent=2))
