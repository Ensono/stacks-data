from pathlib import Path
import os 



def get_os_path_separator():
    """Get the appropriate path separator for the current operating system."""
    if os.name == 'posix':
        return '/'  # Unix/Linux/macOS
    elif os.name == 'nt':
        return '\\'  # Windows
    else:
        return '/'  # Default to '/' for other systems

# Example usage:
path_separator = get_os_path_separator()


TEMPLATES_DIRECTORY = "templates"
workload_type = "example"
template_source_folder = "folder"

# Create the Path object
template_source_path = Path(TEMPLATES_DIRECTORY, workload_type, template_source_folder)

# Ensure the correct separator is added at the end
template_source_path = str(template_source_path) + path_separator

# Example usage:
print(template_source_path)


