import pandas as pd
import json

DATA_TYPES = ["INT", "BIGINT", "VARCHAR(50)", "NVARCHAR(50)", "DATE", "DATETIME", "BIT"]

def bulk_import(file_path):
    """Bulk import schema details from a CSV or Excel file."""
    print("\nReading schema details from file...")
    try:
        if file_path.endswith(".csv"):
            data = pd.read_csv(file_path)
        elif file_path.endswith(".xlsx"):
            data = pd.read_excel(file_path)
        else:
            print("Unsupported file format. Please provide a CSV or Excel file.")
            return None
        
        required_columns = ["Table Name", "Column Name", "Data Type", "Nullable", "Primary Key", "Unique"]
        for col in required_columns:
            if col not in data.columns:
                print(f"Missing required column: {col}")
                return None
        
        tables = {}
        for _, row in data.iterrows():
            table_name = row["Table Name"]
            if table_name not in tables:
                tables[table_name] = {"columns": [], "indexes": [], "comment": row.get("Table Comment", None)}
            
            column = {
                "name": row["Column Name"],
                "type": row["Data Type"],
                "notNull": str(row["Nullable"]).lower() == "no",
                "primaryKey": str(row["Primary Key"]).lower() == "yes",
                "unique": str(row["Unique"]).lower() == "yes",
                "autoIncrement": str(row.get("Auto Increment", "")).lower() == "yes",
                "default": row.get("Default", None),
                "comment": row.get("Comment", None)
            }
            tables[table_name]["columns"].append(column)
        
        print(f"Successfully imported {len(tables)} tables.")
        return tables
    except Exception as e:
        print(f"Error during import: {e}")
        return None

def collect_columns():
    """Manually collect column information for a table."""
    columns = []
    while True:
        print("\nEnter column details:")
        name = input("Column Name: ").strip()
        if not name:
            print("Column Name cannot be empty.")
            continue

        print("\nSelect a data type:")
        for i, dtype in enumerate(DATA_TYPES, 1):
            print(f"{i}. {dtype}")
        dtype_choice = int(input("Choose a data type (number): "))
        data_type = DATA_TYPES[dtype_choice - 1]

        not_null = input("Is NOT NULL? (yes/no): ").strip().lower() == "yes"
        primary_key = input("Is Primary Key? (yes/no): ").strip().lower() == "yes"
        unique = input("Is Unique? (yes/no): ").strip().lower() == "yes"
        auto_increment = input("Is Auto Increment? (yes/no): ").strip().lower() == "yes"
        default = input("Default Value (press Enter to skip): ").strip()
        comment = input("Comment (press Enter to skip): ").strip()

        column = {
            "name": name,
            "type": data_type,
            "notNull": not_null,
            "primaryKey": primary_key,
            "unique": unique,
            "autoIncrement": auto_increment,
            "default": default if default else None,
            "comment": comment if comment else None
        }
        columns.append(column)

        more_columns = input("\nAdd another column? (yes/no): ").strip().lower()
        if more_columns != "yes":
            break
    return columns

def validate_relationship(start_table, start_column, end_table, end_column, schema):
    """Validate that referenced tables and columns exist."""
    table_names = {table["name"]: table for table in schema["tables"]}
    if start_table not in table_names or end_table not in table_names:
        return False, f"One of the tables '{start_table}' or '{end_table}' does not exist."
    start_columns = {col["name"] for col in table_names[start_table]["columns"]}
    end_columns = {col["name"] for col in table_names[end_table]["columns"]}
    if start_column not in start_columns:
        return False, f"Column '{start_column}' does not exist in table '{start_table}'."
    if end_column not in end_columns:
        return False, f"Column '{end_column}' does not exist in table '{end_table}'."
    return True, None

def collect_relationships(schema):
    """Manually collect relationships between tables."""
    relationships = []
    while True:
        print("\nEnter relationship details:")
        name = input("Relationship Name: ").strip()
        start_table = input("Start Table: ").strip()
        start_column = input("Start Column: ").strip()
        end_table = input("End Table: ").strip()
        end_column = input("End Column: ").strip()
        relationship_type = input("Relationship Type (one-to-many, many-to-one, etc.): ").strip()

        valid, error = validate_relationship(start_table, start_column, end_table, end_column, schema)
        if not valid:
            print(f"Error: {error}")
            continue

        relationship = {
            "name": name,
            "start": {"table": start_table, "column": start_column},
            "end": {"table": end_table, "column": end_column},
            "type": relationship_type
        }
        relationships.append(relationship)

        more_relationships = input("\nAdd another relationship? (yes/no): ").strip().lower()
        if more_relationships != "yes":
            break
    return relationships

def main():
    """Main function for generating the ERD JSON schema."""
    print("Welcome to the ERD JSON Generator!")
    erd_schema = {"tables": [], "relationships": []}

    import_choice = input("\nDo you want to import schema from a file? (yes/no): ").strip().lower()
    if import_choice == "yes":
        file_path = input("Enter the file path (CSV/Excel): ").strip()
        imported_schema = bulk_import(file_path)
        if imported_schema:
            for table_name, details in imported_schema.items():
                erd_schema["tables"].append({"name": table_name, **details})

    more_tables = input("\nDo you want to add tables manually? (yes/no): ").strip().lower()
    if more_tables == "yes":
        while True:
            table_name = input("\nEnter Table Name: ").strip()
            columns = collect_columns()
            table_comment = input("Table Comment (press Enter to skip): ").strip()

            erd_schema["tables"].append({
                "name": table_name,
                "columns": columns,
                "indexes": [],
                "comment": table_comment
            })

            add_more = input("Add another table? (yes/no): ").strip().lower()
            if add_more != "yes":
                break

    relationships = collect_relationships(erd_schema)
    erd_schema["relationships"].extend(relationships)

    output_file = input("\nEnter output JSON file name (e.g., schema.json): ").strip()
    if not output_file.endswith(".json"):
        output_file += ".json"
    with open(output_file, "w") as f:
        json.dump(erd_schema, f, indent=4)
    print(f"Generated JSON file: {output_file}")

if __name__ == "__main__":
    main()
