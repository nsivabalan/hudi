"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[56723],{15680:(e,a,t)=>{t.d(a,{xA:()=>c,yg:()=>g});var r=t(96540);function i(e,a,t){return a in e?Object.defineProperty(e,a,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[a]=t,e}function o(e,a){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);a&&(r=r.filter((function(a){return Object.getOwnPropertyDescriptor(e,a).enumerable}))),t.push.apply(t,r)}return t}function n(e){for(var a=1;a<arguments.length;a++){var t=null!=arguments[a]?arguments[a]:{};a%2?o(Object(t),!0).forEach((function(a){i(e,a,t[a])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):o(Object(t)).forEach((function(a){Object.defineProperty(e,a,Object.getOwnPropertyDescriptor(t,a))}))}return e}function s(e,a){if(null==e)return{};var t,r,i=function(e,a){if(null==e)return{};var t,r,i={},o=Object.keys(e);for(r=0;r<o.length;r++)t=o[r],a.indexOf(t)>=0||(i[t]=e[t]);return i}(e,a);if(Object.getOwnPropertySymbols){var o=Object.getOwnPropertySymbols(e);for(r=0;r<o.length;r++)t=o[r],a.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(i[t]=e[t])}return i}var l=r.createContext({}),u=function(e){var a=r.useContext(l),t=a;return e&&(t="function"==typeof e?e(a):n(n({},a),e)),t},c=function(e){var a=u(e.components);return r.createElement(l.Provider,{value:a},e.children)},p="mdxType",d={inlineCode:"code",wrapper:function(e){var a=e.children;return r.createElement(r.Fragment,{},a)}},m=r.forwardRef((function(e,a){var t=e.components,i=e.mdxType,o=e.originalType,l=e.parentName,c=s(e,["components","mdxType","originalType","parentName"]),p=u(t),m=i,g=p["".concat(l,".").concat(m)]||p[m]||d[m]||o;return t?r.createElement(g,n(n({ref:a},c),{},{components:t})):r.createElement(g,n({ref:a},c))}));function g(e,a){var t=arguments,i=a&&a.mdxType;if("string"==typeof e||i){var o=t.length,n=new Array(o);n[0]=m;var s={};for(var l in a)hasOwnProperty.call(a,l)&&(s[l]=a[l]);s.originalType=e,s[p]="string"==typeof e?e:i,n[1]=s;for(var u=2;u<o;u++)n[u]=t[u];return r.createElement.apply(null,n)}return r.createElement.apply(null,t)}m.displayName="MDXCreateElement"},30773:(e,a,t)=>{t.r(a),t.d(a,{assets:()=>l,contentTitle:()=>n,default:()=>d,frontMatter:()=>o,metadata:()=>s,toc:()=>u});var r=t(58168),i=(t(96540),t(15680));const o={title:"Column File Formats: How Hudi Leverages Parquet and ORC ",excerpt:"Explains how Hudi uses Parquet and ORC",author:"Albert Wong",category:"blog",image:"/assets/images/blog/hudi-parquet-orc.jpg",tags:["Data Lake","Apache Hudi","Apache Parquet","Apache ORC"]},n=void 0,s={permalink:"/blog/2024/07/31/hudi-file-formats",editUrl:"https://github.com/apache/hudi/edit/asf-site/website/blog/blog/2024-07-31-hudi-file-formats.md",source:"@site/blog/2024-07-31-hudi-file-formats.md",title:"Column File Formats: How Hudi Leverages Parquet and ORC ",description:"Introduction",date:"2024-07-31T00:00:00.000Z",formattedDate:"July 31, 2024",tags:[{label:"Data Lake",permalink:"/blog/tags/data-lake"},{label:"Apache Hudi",permalink:"/blog/tags/apache-hudi"},{label:"Apache Parquet",permalink:"/blog/tags/apache-parquet"},{label:"Apache ORC",permalink:"/blog/tags/apache-orc"}],readingTime:3.91,truncated:!1,authors:[{name:"Albert Wong"}],nextItem:{title:"Understanding Data Lake Change Data Capture",permalink:"/blog/2024/07/30/data-lake-cdc"}},l={authorsImageUrls:[void 0]},u=[{value:"Introduction",id:"introduction",children:[],level:2},{value:"How does data storage work in Apache Hudi",id:"how-does-data-storage-work-in-apache-hudi",children:[],level:2},{value:"Parquet vs ORC for your Apache Hudi Base File",id:"parquet-vs-orc-for-your-apache-hudi-base-file",children:[{value:"Apache Parquet",id:"apache-parquet",children:[],level:3},{value:"Optimized Row Columnar (ORC)",id:"optimized-row-columnar-orc",children:[],level:3}],level:2},{value:"Choosing the Right Format:",id:"choosing-the-right-format",children:[],level:2},{value:"Conclusion",id:"conclusion",children:[],level:2}],c={toc:u},p="wrapper";function d(e){let{components:a,...t}=e;return(0,i.yg)(p,(0,r.A)({},c,t,{components:a,mdxType:"MDXLayout"}),(0,i.yg)("h2",{id:"introduction"},"Introduction"),(0,i.yg)("p",null,"Apache Hudi emerges as a game-changer in the big data ecosystem by transforming data lakes into transactional hubs. Unlike traditional data lakes which struggle with updates and deletes, Hudi empowers users with functionalities like data ingestion, streaming updates (upserts), and even deletions. This allows for efficient incremental processing, keeping your data pipelines agile and data fresh for real-time analytics. Hudi seamlessly integrates with existing storage solutions and boasts compatibility with popular columnar file formats like ",(0,i.yg)("a",{parentName:"p",href:"https://parquet.apache.org/"},"Parquet")," and ",(0,i.yg)("a",{parentName:"p",href:"https://orc.apache.org/"},"ORC"),". Choosing the right file format is crucial for optimized performance and efficient data manipulation within Hudi, as it directly impacts processing speed and storage efficiency. This blog will delve deeper into these features, and explore the significance of file format selection."),(0,i.yg)("h2",{id:"how-does-data-storage-work-in-apache-hudi"},"How does data storage work in Apache Hudi"),(0,i.yg)("p",null,(0,i.yg)("img",{parentName:"p",src:"https://miro.medium.com/v2/resize:fit:600/format:webp/0*_NFdQLaRGiqDuK3V.png",alt:"Hudi COW MOR"})),(0,i.yg)("p",null,"Apache Hudi offers two table storage options: Copy-on-Write (COW) and Merge-on-Read (MOR)."),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("a",{parentName:"li",href:"https://hudi.apache.org/docs/table_types#copy-on-write-table"},"COW tables"),":",(0,i.yg)("ul",{parentName:"li"},(0,i.yg)("li",{parentName:"ul"},"Data is stored in base files, with Parquet and ORC being the supported formats."),(0,i.yg)("li",{parentName:"ul"},"Updates involve rewriting the entire base file with the modified data."))),(0,i.yg)("li",{parentName:"ul"},(0,i.yg)("a",{parentName:"li",href:"https://hudi.apache.org/docs/table_types#merge-on-read-table"},"MOR tables"),":",(0,i.yg)("ul",{parentName:"li"},(0,i.yg)("li",{parentName:"ul"},"Data resides in base files, again supporting Parquet and ORC formats."),(0,i.yg)("li",{parentName:"ul"},"Updates are stored in separate delta files (using Apache Avro format) and later merged with the base file by a periodic compaction process in the background.")))),(0,i.yg)("h2",{id:"parquet-vs-orc-for-your-apache-hudi-base-file"},"Parquet vs ORC for your Apache Hudi Base File"),(0,i.yg)("p",null,"Choosing the right file format for your Hudi environment depends on your specific needs. Here's a breakdown of Parquet, and ORC along with their strengths, weaknesses, and ideal use cases within Hudi:"),(0,i.yg)("h3",{id:"apache-parquet"},"Apache Parquet"),(0,i.yg)("p",null,(0,i.yg)("a",{parentName:"p",href:"https://parquet.apache.org/"},"Apache Parquet")," is a columnar storage file format. It\u2019s designed for efficiency and performance, and it\u2019s particularly well-suited for running complex queries on large datasets."),(0,i.yg)("p",null,"Pros of Parquet:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},"Columnar Storage: Unlike row-based files, Parquet is columnar-oriented. This means it stores data by columns, which allows for more efficient disk I/O and compression. It reduces the amount of data transferred from disk to memory, leading to faster query performance."),(0,i.yg)("li",{parentName:"ul"},"Compression: Parquet has good compression and encoding schemes. It reduces the disk storage space and improves performance, especially for columnar data retrieval, which is a common case in data analytics.")),(0,i.yg)("p",null,"Cons of Parquet:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},"Write-heavy Workloads: Since Parquet performs column-wise compression and encoding, the cost of writing data can be high for write-heavy workloads."),(0,i.yg)("li",{parentName:"ul"},"Small Data Sets: Parquet may not be the best choice for small datasets because the advantages of its columnar storage model aren\u2019t as pronounced.")),(0,i.yg)("p",null,"Use Cases for Parquet:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},"Parquet is an excellent choice when dealing with large, complex, and nested data structures, especially for read-heavy workloads. Its columnar storage approach makes it an excellent choice for data lakehouse solutions where aggregation queries are common.")),(0,i.yg)("h3",{id:"optimized-row-columnar-orc"},"Optimized Row Columnar (ORC)"),(0,i.yg)("p",null,(0,i.yg)("a",{parentName:"p",href:"https://orc.apache.org/"},"Apache ORC")," is another popular file format that is self-describing, and type-aware columnar file format."),(0,i.yg)("p",null,"Pros of ORC:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},"Compression: ORC provides impressive compression rates that minimize storage space. It also includes lightweight indexes stored within the file, helping to improve read performance."),(0,i.yg)("li",{parentName:"ul"},"Complex Types: ORC supports complex types, including structs, lists, maps, and union types.")),(0,i.yg)("p",null,"Cons of ORC:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},"Less Community Support: Compared to Parquet, ORC has less community support, meaning fewer resources, libraries, and tools for this file format."),(0,i.yg)("li",{parentName:"ul"},"Write Costs: Similar to Parquet, ORC may have high write costs due to its columnar nature.")),(0,i.yg)("p",null,"Use Cases for ORC:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},"ORC is commonly used in cases where high-speed writing is necessary.")),(0,i.yg)("h2",{id:"choosing-the-right-format"},"Choosing the Right Format:"),(0,i.yg)("ul",null,(0,i.yg)("li",{parentName:"ul"},"Prioritize query performance: If complex analytical queries are your primary use case, Parquet is the clear winner due to its superior columnar access."),(0,i.yg)("li",{parentName:"ul"},"Balance performance and cost: ORC offers a good balance between read/write performance and compression, making it suitable for general-purpose data storage in Hudi.")),(0,i.yg)("p",null,"Remember, the best format depends on your specific Hudi application. Consider your workload mix, and performance requirements to make an informed decision."),(0,i.yg)("h2",{id:"conclusion"},"Conclusion"),(0,i.yg)("p",null,"In conclusion, understanding file formats is crucial for optimizing your Hudi data management. Parquet for COW and MOR tables excels in analytical queries with its columnar storage and rich metadata. ORC for COW and MOR tables strikes a balance between read/write performance and compression for general-purpose storage. Avro comes into play for storing delta table data in MOR tables. By considering these strengths, you can make informed decisions on file formats to best suit your big data workloads within the Hudi framework.   "),(0,i.yg)("p",null,"Unleash the power of Apache Hudi for your big data challenges! Head over to ",(0,i.yg)("a",{parentName:"p",href:"https://hudi.apache.org/"},"https://hudi.apache.org/")," and dive into the quickstarts to get started. Want to learn more? Join our vibrant Hudi community! Attend the monthly Community Call or hop into the Apache Hudi Slack to ask questions and gain deeper insights."))}d.isMDXComponent=!0}}]);