def get_dbstring(names):
    """ Convert a list of names to a database string """
    # Ensure `names` is a list of strings
    if isinstance(names, list):
        # Create a comma-separated string of quoted names
        names_str = ",".join(f"'{name}'" for name in names)
    else:
        # If `names` is already a string, ensure it is properly quoted
        names_str = f"'{names}'"
    
    return names_str