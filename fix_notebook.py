import json
import sys

# Script to fix execution_count issues in Jupyter notebooks
def fix_notebook_execution_counts(notebook_path):
    """Fix missing or null execution_count in notebook cells."""
    
    # Load the notebook
    with open(notebook_path, 'r') as f:
        notebook = json.load(f)
    
    # Fix execution counts
    execution_counter = 1
    for cell in notebook['cells']:
        if cell['cell_type'] == 'code':
            # If execution_count is missing or null, assign a sequential number
            if 'execution_count' not in cell or cell['execution_count'] is None:
                cell['execution_count'] = execution_counter
                execution_counter += 1
            else:
                # Update counter to be after the highest existing count
                execution_counter = max(execution_counter, cell['execution_count'] + 1)
    
    # Save the fixed notebook
    with open(notebook_path, 'w') as f:
        json.dump(notebook, f, indent=1)
    
    print(f"✅ Fixed execution counts in {notebook_path}")

if __name__ == "__main__":
    fix_notebook_execution_counts('news_scouting.ipynb')