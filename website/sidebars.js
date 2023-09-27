/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * This source code is licensed under the MIT license found in the
 * LICENSE file in the root directory of this source tree.
 */

module.exports = {
    docs: [
        'overview',
        {
            type: 'category',
            label: 'Quick Start',
            collapsed: false,
            items: [
                'quick-start-guide',
                'flink-quick-start-guide',
                'docker_demo'
            ],
        },
        {
            type: 'category',
            label: 'Concepts',
            items: [
                'timeline',
                'table_types',
                'indexing',
                'file_layouts',
                'metadata',
                'write_operations',
                'schema_evolution',
                'key_generation',
                'concurrency_control',
                'record_payload'
            ],
        },
        {
            type: 'category',
            label: 'How To',
            items: [
                {
                    type: 'category',
                    label: 'SQL',
                    items: [
                        'sql_ddl',
                        'sql_dml',
                        'sql_queries',
                        'procedures'
                    ],
                },
                'writing_data',
                'hoodie_streaming_ingestion',
                {
                    type: 'category',
                    label: 'Syncing to Catalogs',
                    items: [
                        'syncing_aws_glue_data_catalog',
                        'syncing_datahub',
                        'syncing_metastore',
                        "gcp_bigquery"
                    ],
                }
            ],
        },
        {
            type: 'category',
            label: 'Services',
            items: [
                'migration_guide',
                'compaction',
                'clustering',
                'metadata_indexing',
                'hoodie_cleaner',
                'transforms',
                'markers',
                'file_sizing',
                'disaster_recovery',
                'snapshot_exporter',
                'precommit_validator',
            ],
        },
        {
            type: 'category',
            label: 'Configurations',
            items: [
                'basic_configurations',
                'configurations',
            ],
        },
        {
            type: 'category',
            label: 'Guides',
            items: [
                'performance',
                'deployment',
                'cli',
                'metrics',
                'encryption',
                'troubleshooting',
                'tuning-guide',
                'flink_tuning',
                {
                    type: 'category',
                    label: 'Storage Configurations',
                    items: [
                        'cloud',
                        's3_hoodie',
                        'gcs_hoodie',
                        'oss_hoodie',
                        'azure_hoodie',
                        'cos_hoodie',
                        'ibm_cos_hoodie',
                        'bos_hoodie',
                        'jfs_hoodie',
                        'oci_hoodie'
                    ],
                },
            ],
        },
        'use_cases',
        'faq',
        'privacy',
    ],
    quick_links: [
        {
            type: 'link',
            label: 'Powered By',
            href: 'powered-by',
        },
        {
            type: 'link',
            label: 'Chat with us on Slack',
            href: 'https://join.slack.com/t/apache-hudi/shared_invite/zt-20r833rxh-627NWYDUyR8jRtMa2mZ~gg',
        },
    ],
};