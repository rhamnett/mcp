# Object Manager

A unified system for managing all Snowflake objects through just 3 universal tools: `create_object`, `list_objects`, and `drop_object`.

---

## How It Works

Instead of having separate tools for each object type (create_database, create_schema, create_warehouse...), this module provides **3 dynamic tools** that work with ALL Snowflake objects:

```
┌─────────────────┐
│  3 Universal    │
│     Tools       │
├─────────────────┤
│ • create_object │
│ • list_objects  │
│ • drop_object   │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Registry     │  ← Defines what each object type needs
├─────────────────┤
│ • databases     │
│ • schemas       │
│ • warehouses    │
│ • roles         │
│ • tables        │
│ • views         │
│ • functions     │
│ • procedures    │
└────────┬────────┘
         │
         ▼
┌─────────────────┐
│    Managers     │  ← Handles the actual Snowflake operations
├─────────────────┤
│ • CoreManager   │  (for simple objects)
│ • TableManager  │  (for complex objects)
│ • ViewManager   │
│ • ...           │
└─────────────────┘
```

---

## Supported Objects

- **databases** - Data storage containers
- **schemas** - Logical groupings within databases  
- **warehouses** - Compute resources
- **roles** - Access control
- **database_roles** - Database-specific roles
- **tables** - Data tables with columns
- **views** - Virtual tables from queries
- **functions** - Reusable code blocks
- **procedures** - Stored procedures

---

## How the Three Tools Work

Think of these tools like a universal remote control for your Snowflake database. Instead of having dozens of buttons (one for each object type), you have just 3 buttons that adapt based on what you're controlling.

<br>

### **create_object** - The Universal Builder

**What it does:** Creates ANY type of Snowflake object by understanding what that object needs.

**The Process:**
1. You specify the object type (database, table, warehouse, etc.)
2. The tool looks up what that object requires
3. It validates your inputs match the requirements
4. It creates the object with the right settings

**Common Parameters by Object Type:**

| Object Type | Essential Info | Optional Settings |
|------------|---------------|-------------------|
| **Database** | Name | Comment, retention days, transient flag |
| **Warehouse** | Name | Size (X-Small to 6X-Large), auto-suspend time, clustering |
| **Table** | Name, columns, location | Clustering keys, data retention, comments |
| **Role** | Name | Comment, granted privileges |
| **Schema** | Name, database | Comment, data retention |

<br>

### **list_objects** - The Universal Finder

**What it does:** Finds and lists ANY type of Snowflake object with smart filtering.

**Search Options:**
- **No filter** → Shows everything you have access to
- **Pattern matching** → Find objects with names LIKE a pattern
- **Prefix search** → Find objects that start with specific text
- **Hierarchy filtering** → List objects within a specific database or schema
- **Result limiting** → Control how many results you get back

**Think of it like:** A smart search engine that knows the structure of your Snowflake account.

<br>

### **drop_object** - The Universal Remover

**What it does:** Safely removes ANY type of Snowflake object with built-in protections.

**Safety Features:**
- **IF EXISTS mode** → Won't fail if the object is already gone
- **CASCADE option** → Can remove an object and everything inside it
- **System protection** → Won't let you drop critical system objects
- **Confirmation required** → For dangerous operations like CASCADE

**Like recycling:** You specify what to remove, whether to empty it first (CASCADE), and whether it's okay if it's already gone (IF EXISTS).

---

## Adding New Object Types

Think of adding a new object type like teaching the system about a new kind of building block in your Snowflake world.

### The Three-Step Process

<br>

#### Step 1: Define the Blueprint
Tell the system what your new object type looks like:
- **What to call it** (e.g., "stream", "task", "pipe")
- **Where it lives** (standalone or inside a database/schema)
- **What information it needs** (required and optional parameters)
- **What rules apply** (size limits, naming conventions, dependencies)

<br>

#### Step 2: Choose the Handler

**Simple Objects** (like databases, roles):
- Use the standard handler
- Just needs basic create/list/drop operations
- Examples: databases, schemas, warehouses, roles

**Complex Objects** (like tables, procedures):
- Need a custom handler
- Have special requirements (columns for tables, code for functions)
- Require extra validation or transformation
- Examples: tables, views, functions, procedures

<br>

#### Step 3: Connect to the System
Register your new object type so the three universal tools know about it:
- The tools automatically adapt to handle your new object
- No changes needed to the tools themselves
- Users can immediately start using create_object, list_objects, and drop_object with your new type

<br>

### Real-World Analogy

It's like adding a new appliance to a smart home system:
1. **Define** what the appliance is and what settings it has
2. **Decide** if it needs special handling (complex) or works with standard commands (simple)
3. **Register** it with the home automation system

Once registered, you can control it with the same universal remote (our 3 tools) that controls everything else!

---

## Directory Structure

```
object_manager/
├── README.md           # This file
├── registry.py         # Object configurations
├── dynamic.py          # Tool factory
├── tools/
│   ├── create.py      # Universal create tool
│   ├── list.py        # Universal list tool
│   └── drop.py        # Universal drop tool
└── managers/
    ├── core.py        # Base manager for simple objects
    ├── table.py       # Table-specific logic
    ├── view.py        # View-specific logic
    ├── function.py    # Function-specific logic
    └── procedure.py   # Procedure-specific logic
```

---

## Key Benefits

- **Simplicity**: Just 3 tools to learn instead of dozens
- **Consistency**: Same interface for all objects
- **Extensibility**: Easy to add new object types
- **Type Safety**: Parameter validation per object type
- **Error Handling**: Unified error messages with HTTP codes

---

## How It All Fits Together

**The Flow of Information:**

1. **You Ask** → "Create a warehouse called 'analytics_wh' that's MEDIUM sized"
2. **Tool Receives** → The create_object tool gets your request
3. **Registry Checks** → "What does a warehouse need? What rules apply?"
4. **Validation** → "Is MEDIUM a valid size? Is the name allowed?"
5. **Manager Executes** → The right handler talks to Snowflake
6. **Response** → "Success! Warehouse 'analytics_wh' created"

**Why This Design?**

- **One Interface, Many Objects** → Like a Swiss Army knife - one tool, many functions
- **Smart Validation** → Catches errors before they reach Snowflake
- **Proper Error Messages** → Tells you exactly what went wrong and how to fix it
- **Future-Proof** → New Snowflake features can be added without changing the tools
- **Consistent Experience** → Whether creating a database or a complex table, the process feels the same