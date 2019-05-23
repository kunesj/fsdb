# FSDB (Filesystem Database)

Extremely dumb and minimalistic database.

* Saves tables and records in folder tree on disk
* Aims to use least processing power when idle
* All database files are human readable
* low CPU, low RAM, higher DB disk size, low speed

## TODO

* implement required fields (attribute already implemented)
* implement unique fields (attribute already implemented)
* transactions and/or multi-threaded access (maybe quick dirty fix that makes new connections wait until is free???)
* run as db server -> connections, open/close database, ...

## Example

Database configuration given to `Manager.init_from_config(config)`

```
[
  {
    "name": "website_db",
    "tables": [
      {
        "name": "users",
        "fields": [
          {"name": "id", "type": "int"},
          {"name": "login", "type": "str", "required": true, "unique": true},
          {"name": "password", "type": "str", "required": true}
        ],
        "records": [
          {
            "id": 1,
            "login": "USER_NAME",
            "password": "DEFAULT_PASSWORD"
          }
        ]
      },
      {
        "name": "blog_posts",
        "fields": [
          {"name": "id", "type": "datetime"},
          {"name": "name", "type": "str", "required": true, "default": "Title"},
          {"name": "text_md", "type": "file"},
          {"name": "text_html", "type": "file"},
          {"name": "files", "type": "file_list"},
          {"name": "author", "type": "str"},
          {"name": "public", "type": "bool", "default": false}
        ]
      },
      {
        "name": "pages",
        "fields": [
          {"name": "id", "type": "int"},
          {"name": "name", "type": "str", "required": true, "default": "Title"},
          {"name": "text_md", "type": "file"},
          {"name": "text_html", "type": "file"},
          {"name": "files", "type": "file_list"},
          {"name": "author", "type": "str"},
          {"name": "public", "type": "bool", "default": false}
        ]
      }
    ]
  }
]
```

Result (after a bit of use)

```
WEBSERVER
└── website_db
    ├── blog_posts
    │   ├── 2015-04-16T13-18-00.000000
    │   │   ├── data.json
    │   │   ├── text.html
    │   │   └── text.md
    │   ├── 2015-04-16T16-25-00.000000
    │   │   ├── data.json
    │   │   ├── text.html
    │   │   └── text.md
    │   ├── 2015-05-08T21-51-00.000000
    │   │   ├── data.json
    │   │   ├── files
    │   │   │   ├── ct24_1.png
    │   │   │   ├── ct24_2b.png
    │   │   │   └── ct24_3.png
    │   │   ├── text.html
    │   │   └── text.md
    │   ├── 2015-05-28T03-08-00.000000
    │   │   ├── data.json
    │   │   ├── text.html
    │   │   └── text.md
    │   ├── 2015-06-01T23-58-00.000000
    │   │   ├── data.json
    │   │   ├── files
    │   │   │   └── pyinstaller_makefile.png
    │   │   ├── text.html
    │   │   └── text.md
    │   └── data.json
    ├── data.json
    ├── pages
    │   ├── 1
    │   │   ├── data.json
    │   │   ├── files
    │   │   │   └── omad_preview.png
    │   │   ├── text.html
    │   │   └── text.md
    │   ├── 2
    │   │   ├── data.json
    │   │   ├── text.html
    │   │   └── text.md
    │   ├── 3
    │   │   ├── data.json
    │   │   ├── text.html
    │   │   └── text.md
    │   ├── 4
    │   │   ├── data.json
    │   │   ├── text.html
    │   │   └── text.md
    │   └── data.json
    └── users
        ├── 1
        │   └── data.json
        └── data.json
```
