from __future__ import annotations
# -*- coding: utf-8 -*-
# code generated by Prisma. DO NOT EDIT.
# fmt: off
# -- template metadata.py.jinja --


PRISMA_MODELS: set[str] = {
    'dataset',
    'dataset_identifier',
    'dataset_version',
    'data_branch',
}

RELATIONAL_FIELD_MAPPINGS: dict[str, dict[str, str]] = {
    'dataset': {
        'dataset_identifier': 'dataset_identifier',
        'dataset_version': 'dataset_version',
    },
    'dataset_identifier': {
        'dataset': 'dataset',
    },
    'dataset_version': {
        'branch': 'data_branch',
        'dataset': 'dataset',
    },
    'data_branch': {
        'dataset_version': 'dataset_version',
    },
}

# fmt: on