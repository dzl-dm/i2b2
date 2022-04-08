#!/usr/bin/env python3
""" compareCsv.py

For i2b2 database table exports, compare 2 versions - good for regression testing.

Run as a script, provide 2 file names as args.
Please use CSV files with a headers line

Run in windows with something like:
```/c/Program\ Files/Python39/python ~/projects/worktrees/i2b2-meta-python/meta-python/support/compareCsv.py```
"""

import logging
import logging.config
import os
import yaml
with open("{}/config/scriptlog.yaml".format(os.path.dirname(os.path.realpath(__file__))), "r") as f:
    log_config = yaml.load(f, Loader=yaml.FullLoader)
logging.config.dictConfig(log_config)
logger = logging.getLogger(__name__)

import csv

## Use these as prefixes for the filenames
sources:dict[str:str] = {"dzl": "test", "local": "DZL"}
## Use this
suffix:str = "slim"
## Which columns to keep:
table_cols_to_keep:dict = {
    "i2b2metadata.i2b2": ["c_hlevel", "c_fullname", "c_name", "c_synonym_cd", "c_visualattributes", "c_basecode", "c_facttablecolumn", "c_tablename", "c_columnname", "c_columndatatype", "c_operator", "c_dimcode", "c_comment", "c_tooltip", "m_applied_path", "valuetype_cd", "m_exclusion_cd", "c_path", "c_symbol"],
    "i2b2metadata.table_access": ["c_table_cd", "c_table_name", "c_protected_access", "c_hlevel", "c_fullname", "c_name", "c_synonym_cd", "c_visualattributes", "c_totalnum", "c_basecode", "c_metadataxml", "c_facttablecolumn", "c_dimtablename", "c_columnname", "c_columndatatype", "c_operator", "c_dimcode", "c_comment", "c_tooltip", "c_entry_date", "c_change_date", "c_status_cd", "valuetype_cd", "c_ontology_protection"]
}

def trim_data(source_label:str, source_cd:str, table_cols_to_keep:dict[str:list[str]], suffix:str, dir:str = None):
    """Read and re-write CSV files with fewer columns and fewer rows (by selecting only the same source)"""
    logger.debug("Current dir: {}".format(os.getcwd()))
    for table_name, cols in table_cols_to_keep.items():
        in_file = "{}.{}.csv".format(source_label, table_name)
        tmp_file = "{}.{}.temp.csv".format(source_label, table_name)
        out_file = "{}.{}.{}.csv".format(source_label, table_name, suffix)
        if not os.path.isfile(in_file):
            logger.error("Input file ({}) doesn't exist!".format(in_file))
            return False
        with open(in_file, 'r') as base, open(tmp_file, 'w') as unsorted_file:
            reader = csv.DictReader(base, skipinitialspace=True, delimiter = ";")
            output_fields = [x for x in reader.fieldnames if x in cols]
            logger.debug("Fieldnames from reader vs output_fields:\n{}\n{}".format(reader.fieldnames, output_fields))
            writer = csv.DictWriter(unsorted_file, fieldnames=list(output_fields), delimiter = ";")
            logger.debug("Writing relevant, shortened lines...")
            writer.writeheader()
            for line in reader:
                # logger.debug("Line is: {}".format(line))
                trimmed_line = {k:v for k,v in line.items() if k in cols}
                if (
                        "sourcesystem_cd" in line and source_cd in line["sourcesystem_cd"]
                    ) or (
                        "c_table_cd" in line and source_cd in line["c_table_cd"]
                    ) or (
                        table_name == "i2b2metadata.table_access"
                    ):
                    writer.writerow(trimmed_line)
        logger.debug("Temp file written...")
        ## Sort files
        with open(tmp_file, 'r') as unsorted_file, open(out_file, 'w') as prepared_file:
            reader = csv.DictReader(unsorted_file, skipinitialspace=True, delimiter=";")
            writer = csv.DictWriter(prepared_file, fieldnames = reader.fieldnames, delimiter=";")
            sortby_col = reader.fieldnames[0]
            sortby_col2 = reader.fieldnames[1]
            # logger.debug("Fieldnames (sortby: {}): {}".format(sortby_col, reader.fieldnames))
            sortedlist = sorted(reader, key=lambda row: (row[sortby_col], row[sortby_col2]), reverse=False)
            # row:(row['column_1'],row['column_2'])
            # logger.debug("Sorted csv: {}".format(sortedlist))
            writer.writeheader()
            for row in sortedlist:
                writer.writerow(row)
            # writer.writerows(sortedlist)
        logger.debug("Writing for '{}' complete...".format(out_file))

    pass

def pre_checks():
    """Check variables are within limits"""
    pass

if __name__ == "__main__":
    """Run as script"""
    logger.info("Running CSV comparison script")
    pre_checks()
    for source_label, source_cd in sources.items():
        trim_data(source_label, source_cd, table_cols_to_keep, suffix)
    logger.info("Finished CSV comparison script")
