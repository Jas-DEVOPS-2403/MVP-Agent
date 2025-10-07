"""Streamlit UI entry point for the AML monitoring agent."""
from pathlib import Path

import streamlit as st

from main import run_pipeline


def main() -> None:
    """Render a simple button that triggers the AML pipeline."""
    st.title("Financial AML Agent")
    st.caption("Trigger the end-to-end monitoring pipeline on the sample dataset.")

    if st.button("Run pipeline"):
        summary = run_pipeline()
        st.success("Pipeline execution completed.")
        st.json(summary)

    data_path = Path("data") / "sample.csv"
    st.sidebar.header("Data Overview")
    if data_path.exists():
        st.sidebar.code(data_path.read_text(), language="csv")


if __name__ == "__main__":
    main()
