const darkCodeTheme = require('prism-react-renderer/themes/dracula');
const versions = require('./versions.json');
const VersionsArchived = require('./versionsArchived.json');
const allDocHomesPaths = [
  '/docs/',
  '/docs/next/',
  ...versions.slice(1).map((version) => `/docs/${version}/`),
];

/** @type {import('@docusaurus/types').DocusaurusConfig} */
module.exports = {
  title: 'Apache Hudi',
  tagline: 'Hudi brings transactions, record-level updates/deletes and change streams to data lakes!',
  url: 'https://hudi.apache.org',
  baseUrl: '/',
  onBrokenLinks: 'throw',
  onBrokenMarkdownLinks: 'warn',
  favicon: '/assets/images/favicon.ico',
  organizationName: 'apache',
  projectName: 'hudi',
  customFields: {
    copyrightText:
        "Hudi, Apache and the Apache feather logo are trademarks of The Apache Software Foundation.",
    tagline: "Hudi brings transactions, record-level updates/deletes and change streams to data lakes!",
    taglineConfig: {
      prefix: "Hudi brings ",
      suffix: " to data lakes!",
      content: [
        "transactions",
        "record-level updates/deletes",
        "change streams",
      ],
    },
  },
  i18n: {
    defaultLocale: 'en',
    locales: ['en', 'cn'],
    localeConfigs: {
      en: {
        label: 'English',
        direction: 'ltr',
      },
      cn: {
        label: 'Chinese',
        direction: 'ltr',
      },
    },
  },
  plugins: [
    ['@docusaurus/plugin-content-blog', {
      id: 'video-blog',
      path: 'videoBlog',
      routeBasePath: 'videos',
      blogSidebarCount: 0,
    }],
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'contribute',
        path: 'contribute',
        routeBasePath: 'contribute',
        sidebarPath: require.resolve('./sidebarsContribute.js'),
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'community',
        path: 'community',
        routeBasePath: 'community',
        sidebarPath: require.resolve('./sidebarsCommunity.js'),
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'releases',
        path: 'releases',
        routeBasePath: 'releases',
        sidebarPath: require.resolve('./sidebarsReleases.js'),
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      '@docusaurus/plugin-content-docs',
      {
        id: 'learn',
        path: 'learn',
        routeBasePath: 'learn',
        showLastUpdateAuthor: false,
        showLastUpdateTime: false,
      },
    ],
    [
      '@docusaurus/plugin-client-redirects',
      {
        fromExtensions: ['html'],
        createRedirects: function (path) {
          // redirect to /docs from /docs/introduction,
          // as introduction has been made the home doc
          if (allDocHomesPaths.includes(path)) {
            return [`${path}/quick-start-guide`];
          }
        },
        redirects: [
          {
            from: ['/docs/contribute', '/docs/next/contribute', '/contribute/get-involved'],
            to: '/community/get-involved',
          },
          {
            from: ['/contribute/team'],
            to: '/community/team',
          },
          {
            from: ['/docs/releases', '/docs/next/releases'],
            to: '/releases/release-0.14.0',
          },
          {
            from: ['/releases'],
            to: '/releases/release-0.14.0',
          },
        ],
      },
    ],
  ],
  themeConfig: {
    metadata: [{name: 'keywords', content: 'apache hudi, data lake, lakehouse, big data, apache spark, apache flink, presto, trino, analytics, data engineering'}],
    algolia: {
      apiKey: 'e300f1558b703c001c515c0e7f8e0908',
      indexName: 'apache_hudi',
      contextualSearch: true,
    },
    navbar: {
      logo: {
        alt: 'Apache Hudi',
        src: 'assets/images/hudi.png',
      },
      items: [
        {
          label: 'Docs',
          to: '/docs/overview',
        },
        {
          label: 'Learn',
          position: 'left',
          items: [
            {
              label: 'Talks',
              to: 'talks',
            },
            {
              label: 'Video Guides',
              to: 'videos',
            },
            {
              label: 'FAQ',
              href: '/docs/faq',
            },
            {
              label: 'Tech Specs',
              href: '/tech-specs',
            },
            {
              label: 'Tech Specs 1.0',
              href: '/tech-specs-1point0',
            },
            {
              label: 'Technical Wiki',
              href: 'https://cwiki.apache.org/confluence/display/HUDI',
            }
          ],
        },
        {
          label: 'Contribute',
          position: 'left',
          items: [
            {
              label: 'How to Contribute',
              to: '/contribute/how-to-contribute',
            },
            {
              label: 'Developer Setup',
              to: '/contribute/developer-setup',
            },
            {
              label: 'RFC Process',
              to: '/contribute/rfc-process',
            },
            {
              label: 'Report Security Issues',
              to: '/contribute/report-security-issues',
            },
            {
              label: 'Report Issues',
              href: 'https://issues.apache.org/jira/projects/HUDI/summary',
            }
          ],
        },
        {
          label: 'Community',
          position: 'left',
          items: [
            {
              label: 'Get Involved',
              to: '/community/get-involved',
            },
            {
              label: 'Community Syncs',
              to: '/community/syncs',
            },
            {
              label: 'Office Hours',
              to: '/community/office_hours',
            },
            {
              label: 'Team',
              to: '/community/team',
            }
          ],
        },
        {to: '/blog', label: "Blog", position: 'left'},
        {to: '/powered-by', label: "Who's Using", position: 'left'},
        {to: '/roadmap', label: "Roadmap", position: 'left'},
        {to: '/releases/download', label: 'Download', position: 'left'},
        // right
        {
          type: 'docsVersionDropdown',
          position: 'right',
          dropdownActiveClassDisabled: true,
          dropdownItemsAfter: [
            ...Object.entries(VersionsArchived).map(
                ([versionName, versionUrl]) => ({
                  label: versionName,
                  href: versionUrl,
                }),
            )
          ],
        },
        {
          type: 'localeDropdown',
          position: 'right',
        },
        {
          href: 'https://github.com/apache/hudi',
          position: 'right',
          className: 'header-github-link',
          'aria-label': 'GitHub repository',
        },
        {
          href: 'https://twitter.com/ApacheHudi',
          position: 'right',
          className: 'header-twitter-link',
          'aria-label': 'Hudi Twitter Handle',
        },
        {
          href: 'https://join.slack.com/t/apache-hudi/shared_invite/zt-20r833rxh-627NWYDUyR8jRtMa2mZ~gg',
          position: 'right',
          className: 'header-slack-link',
          'aria-label': 'Hudi Slack Channel',
        },
        {
          href: 'https://www.youtube.com/channel/UCs7AhE0BWaEPZSChrBR-Muw',
          position: 'right',
          className: 'header-youtube-link',
          'aria-label': 'Hudi YouTube Channel',
        },
        {
          href: 'https://www.linkedin.com/company/apache-hudi/?viewAsMember=true',
          position: 'right',
          className: 'header-linkedin-link',
          'aria-label': 'Hudi Linkedin Page',
        },
      ],
    },
    footer: {
      style: 'light',
      links: [
        {
          title: 'About',
          items: [
            {
              label: 'Our Vision',
              to: '/blog/2021/07/21/streaming-data-lake-platform',
            },
            {
              label: 'Concepts',
              to: '/docs/concepts',
            },
            {
              label: 'Team',
              to: '/community/team',
            },
            {
              label: 'Releases',
              to: '/releases/release-0.14.0',
            },
            {
              label: 'Download',
              to: '/releases/download',
            },
            {
              label: 'Who\'s Using',
              to: 'powered-by',
            },
          ],
        },
        {
          title: 'Learn',
          items: [
            {
              label: 'Quick Start',
              to: '/docs/quick-start-guide',
            },
            {
              label: 'Docker Demo',
              to: '/docs/docker_demo',
            },
            {
              label: 'Blog',
              to: '/blog',
            },
            {
              label: 'Talks',
              to: 'talks',
            },
            {
              label: 'Video Guides',
              to: 'videos',
            },
            {
              label: 'FAQ',
              href: '/docs/faq',
            },
            {
              label: 'Technical Wiki',
              href: 'https://cwiki.apache.org/confluence/display/HUDI',
            }
          ],
        },
        {
          title: 'Hudi On Cloud',
          items: [
            {
              label: 'AWS',
              to: '/docs/s3_hoodie',
            },
            {
              label: 'Google Cloud',
              to: '/docs/gcs_hoodie',
            },
            {
              label: 'Alibaba Cloud',
              to: '/docs/oss_hoodie',
            },
            {
              label: 'Microsoft Azure',
              to: '/docs/azure_hoodie',
            },
            {
              label: 'Tencent Cloud',
              to: '/docs/cos_hoodie',
            },
            {
              label: 'IBM Cloud',
              to: '/docs/ibm_cos_hoodie',
            },
          ],
        },
        {
          title: 'Community',
          items: [
            {
              label: 'Get Involved',
              to: '/community/get-involved'
            },
            {
              label: 'Slack',
              href: 'https://join.slack.com/t/apache-hudi/shared_invite/zt-20r833rxh-627NWYDUyR8jRtMa2mZ~gg',
            },
            {
              label: 'GitHub',
              href: 'https://github.com/apache/hudi',
            },
            {
              label: 'Twitter',
              href: 'https://twitter.com/ApacheHudi',
            },
            {
              label: 'YouTube',
              href:  'https://www.youtube.com/channel/UCs7AhE0BWaEPZSChrBR-Muw',
            },
            {
              label: 'Linkedin',
              href: 'https://www.linkedin.com/company/apache-hudi/?viewAsMember=true',
            },
            {
              label: 'Mailing List',
              to: 'mailto:dev-subscribe@hudi.apache.org?Subject=SubscribeToHudi',
            },
          ],
        },
        {
          title: 'Apache',
          items: [
            {
              label: 'Events',
              to: 'https://www.apache.org/events/current-event',
            },
            {
              label: 'Thanks',
              to: 'https://www.apache.org/foundation/thanks.html',
            },
            {
              label: 'License',
              to: 'https://www.apache.org/licenses',
            },
            {
              label: 'Security',
              to: 'https://www.apache.org/security',
            },
            {
              label: 'Sponsorship',
              to: 'https://www.apache.org/foundation/sponsorship.html',
            },
            {
              label: 'Foundation',
              to: 'https://www.apache.org',
            },
          ],
        },
      ],
      logo: {
        alt: 'Apache Hudi™',
        src: '/assets/images/logo-big.png',
        href: 'https://hudi.apache.org/',
      },
      copyright:
          'Copyright © 2021 <a href="https://apache.org">The Apache Software Foundation</a>, Licensed under the <a href="https://www.apache.org/licenses/LICENSE-2.0"> Apache License, Version 2.0</a>. <br />Hudi, Apache and the Apache feather logo are trademarks of The Apache Software Foundation.',
    },
    prism: {
      theme: darkCodeTheme,
      additionalLanguages: ['java', 'scala'],
      prismPath: require.resolve('./src/theme/prism-include-languages.js'),
    },
    announcementBar: {
      id: 'announcementBar-1', // Increment on change
      content:
          '⭐️ If you like Apache Hudi, give it a star on <a target="_blank" rel="noopener noreferrer" href="https://github.com/apache/hudi">GitHub</a>! ⭐',
    },
    colorMode: {
      defaultMode: 'light',
      disableSwitch: true,
    },
    blog: {
      path: 'blog', // Path to the existing blog folder
      routeBasePath: 'blog', // Route for the existing blog
      include: ['*.md', '*.mdx'], // File types to include for the existing blog

      // Add the new blog for videos
      videoBlog: {
        path: 'video-blog', // Path to the video blog folder
        routeBasePath: 'videos', // Route for the video blog
        include: ['*.md', '*.mdx'], // File types to include for the video blog
        videoBlogRoute: '/videos'
        // Add any other specific settings for the video blog
      },
    },
  },
  presets: [
    [
      '@docusaurus/preset-classic',
      {
        docs: {
          sidebarPath: require.resolve('./sidebars.js'),
          // Please change this to your repo.
          editUrl: ({ version, versionDocsDirPath, docPath, locale }) => {
            if (locale != this.defaultLocale) {
              return `https://github.com/apache/hudi/tree/asf-site/website/${versionDocsDirPath}/${docPath}`
            } else {
              return `https://github.com/apache/hudi/tree/asf-site/website/i18n/${locale}/docusaurus-plugin-content-${versionDocsDirPath}/${version}/${docPath}`
            }
          },
          includeCurrentVersion: true,
          versions: {
            current: {
              label: 'Current',
              path: 'next',
              banner: 'unreleased',
            },
            '0.14.0': {
              label: '0.14.0',
              path: '',
            }
          },
        },
        blog: {
          editUrl:
            'https://github.com/apache/hudi/edit/asf-site/website/blog/',
          blogTitle: 'Blogs List Page',
          blogSidebarCount: 0,
          blogSidebarTitle: 'Recent posts',
          /**
           * URL route for the blog section of your site.
           * *DO NOT* include a trailing slash.
           */
          routeBasePath: 'blog',
          include: ['*.md', '*.mdx'],
          postsPerPage: 12,
          /**
           * Theme components used by the blog pages.
           */
          blogListComponent: '@theme/BlogListPage',
          blogPostComponent: '@theme/BlogPostPage',
          blogTagsListComponent: '@theme/BlogTagsListPage',
          blogTagsPostsComponent: '@theme/BlogTagsPostsPage',
          feedOptions: {
            type: "all",
            title: 'Apache Hudi: User-Facing Analytics',
          },
          showReadingTime: true,
        },
        theme: {
          customCss: require.resolve('./src/css/custom.css'),
        },
      },
    ],
  ],
  scripts: [],
  stylesheets: [
    'https://fonts.googleapis.com/css?family=Comfortaa|Ubuntu|Roboto|Source+Code+Pro',
    'https://at-ui.github.io/feather-font/css/iconfont.css',
  ],
};