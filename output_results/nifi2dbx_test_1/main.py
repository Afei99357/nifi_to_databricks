# Databricks notebook source
# Orchestrator notebook
dbutils.widgets.text("STEP_MODULE", "")
step_mod = dbutils.widgets.get("STEP_MODULE")

import importlib.util, sys, os, glob

def run_module(rel_path: str):
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    full_path = os.path.join(root, rel_path)
    if not os.path.exists(full_path):
        raise FileNotFoundError(f"Module not found: {full_path}")
    spec = importlib.util.spec_from_file_location("step_mod", full_path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["step_mod"] = mod
    spec.loader.exec_module(mod)

if step_mod:
    print(f"Running single step: {step_mod}")
    run_module(step_mod)
else:
    root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    steps = sorted(glob.glob(os.path.join(root, "src", "steps", "*.py")))
    print("No STEP_MODULE provided; running all steps sequentially:")
    for s in steps:
        rel = os.path.relpath(s, root)
        print(f" -> {rel}")
        run_module(rel)