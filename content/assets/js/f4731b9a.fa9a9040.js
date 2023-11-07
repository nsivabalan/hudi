"use strict";(self.webpackChunkhudi=self.webpackChunkhudi||[]).push([[84936],{3905:(e,a,t)=>{t.d(a,{Zo:()=>p,kt:()=>h});var r=t(67294);function n(e,a,t){return a in e?Object.defineProperty(e,a,{value:t,enumerable:!0,configurable:!0,writable:!0}):e[a]=t,e}function i(e,a){var t=Object.keys(e);if(Object.getOwnPropertySymbols){var r=Object.getOwnPropertySymbols(e);a&&(r=r.filter((function(a){return Object.getOwnPropertyDescriptor(e,a).enumerable}))),t.push.apply(t,r)}return t}function o(e){for(var a=1;a<arguments.length;a++){var t=null!=arguments[a]?arguments[a]:{};a%2?i(Object(t),!0).forEach((function(a){n(e,a,t[a])})):Object.getOwnPropertyDescriptors?Object.defineProperties(e,Object.getOwnPropertyDescriptors(t)):i(Object(t)).forEach((function(a){Object.defineProperty(e,a,Object.getOwnPropertyDescriptor(t,a))}))}return e}function l(e,a){if(null==e)return{};var t,r,n=function(e,a){if(null==e)return{};var t,r,n={},i=Object.keys(e);for(r=0;r<i.length;r++)t=i[r],a.indexOf(t)>=0||(n[t]=e[t]);return n}(e,a);if(Object.getOwnPropertySymbols){var i=Object.getOwnPropertySymbols(e);for(r=0;r<i.length;r++)t=i[r],a.indexOf(t)>=0||Object.prototype.propertyIsEnumerable.call(e,t)&&(n[t]=e[t])}return n}var c=r.createContext({}),u=function(e){var a=r.useContext(c),t=a;return e&&(t="function"==typeof e?e(a):o(o({},a),e)),t},p=function(e){var a=u(e.components);return r.createElement(c.Provider,{value:a},e.children)},d="mdxType",m={inlineCode:"code",wrapper:function(e){var a=e.children;return r.createElement(r.Fragment,{},a)}},s=r.forwardRef((function(e,a){var t=e.components,n=e.mdxType,i=e.originalType,c=e.parentName,p=l(e,["components","mdxType","originalType","parentName"]),d=u(t),s=n,h=d["".concat(c,".").concat(s)]||d[s]||m[s]||i;return t?r.createElement(h,o(o({ref:a},p),{},{components:t})):r.createElement(h,o({ref:a},p))}));function h(e,a){var t=arguments,n=a&&a.mdxType;if("string"==typeof e||n){var i=t.length,o=new Array(i);o[0]=s;var l={};for(var c in a)hasOwnProperty.call(a,c)&&(l[c]=a[c]);l.originalType=e,l[d]="string"==typeof e?e:n,o[1]=l;for(var u=2;u<i;u++)o[u]=t[u];return r.createElement.apply(null,o)}return r.createElement.apply(null,t)}s.displayName="MDXCreateElement"},6746:(e,a,t)=>{t.d(a,{Z:()=>i});var r=t(67294),n=t(72389);function i(e){let{children:a,url:i}=e;return(0,n.Z)()&&(t.g.window.location.href=i),r.createElement("span",null,a,"or click ",r.createElement("a",{href:i},"here"))}},98304:(e,a,t)=>{t.r(a),t.d(a,{assets:()=>u,contentTitle:()=>l,default:()=>s,frontMatter:()=>o,metadata:()=>c,toc:()=>p});var r=t(87462),n=(t(67294),t(3905)),i=t(6746);const o={title:"Part1: Query apache hudi dataset in an amazon S3 data lake with amazon athena : Read optimized queries",authors:[{name:"Dhiraj Thakur"},{name:"Sameer Goel"},{name:"Imtiaz Sayed"}],category:"blog",image:"/assets/images/blog/2021-07-16-query-hudi-using-athena-ro-queries.png",tags:["how-to","read-optimized-queries","amazon"]},l=void 0,c={permalink:"/blog/2021/07/16/Query-apache-hudi-dataset-in-an-amazon-S3-data-lake-with-amazon-athena-Read-optimized-queries",editUrl:"https://github.com/apache/hudi/edit/asf-site/website/blog/blog/2021-07-16-Query-apache-hudi-dataset-in-an-amazon-S3-data-lake-with-amazon-athena-Read-optimized-queries.mdx",source:"@site/blog/2021-07-16-Query-apache-hudi-dataset-in-an-amazon-S3-data-lake-with-amazon-athena-Read-optimized-queries.mdx",title:"Part1: Query apache hudi dataset in an amazon S3 data lake with amazon athena : Read optimized queries",description:"Redirecting... please wait!!",date:"2021-07-16T00:00:00.000Z",formattedDate:"July 16, 2021",tags:[{label:"how-to",permalink:"/blog/tags/how-to"},{label:"read-optimized-queries",permalink:"/blog/tags/read-optimized-queries"},{label:"amazon",permalink:"/blog/tags/amazon"}],readingTime:.045,truncated:!1,authors:[{name:"Dhiraj Thakur"},{name:"Sameer Goel"},{name:"Imtiaz Sayed"}],prevItem:{title:"Amazon Athena expands Apache Hudi support",permalink:"/blog/2021/07/16/Amazon-Athena-expands-Apache-Hudi-support"},nextItem:{title:"Employing correct configurations for Hudi's cleaner table service",permalink:"/blog/2021/06/10/employing-right-configurations-for-hudi-cleaner"}},u={authorsImageUrls:[void 0,void 0,void 0]},p=[],d={toc:p},m="wrapper";function s(e){let{components:a,...t}=e;return(0,n.kt)(m,(0,r.Z)({},d,t,{components:a,mdxType:"MDXLayout"}),(0,n.kt)(i.Z,{url:"https://aws.amazon.com/blogs/big-data/part-1-query-an-apache-hudi-dataset-in-an-amazon-s3-data-lake-with-amazon-athena-part-1-read-optimized-queries/",mdxType:"Redirect"},"Redirecting... please wait!! "))}s.isMDXComponent=!0}}]);