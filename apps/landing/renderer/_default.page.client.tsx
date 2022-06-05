import React from 'react';
import ReactDOM from 'react-dom';
import type { PageContextBuiltInClient } from 'vite-plugin-ssr/client';
import { getPage } from 'vite-plugin-ssr/client';

import '@sd/ui/style';

import '../style.scss';
import { PageContainer } from './PageContainer';
import type { PageContext } from './types';

(async function hydrate() {
	const pageContext = await getPage<PageContextBuiltInClient & PageContext>();
	const { Page, pageProps } = pageContext;

	ReactDOM.hydrate(
		<PageContainer pageContext={pageContext}>
			<Page {...pageProps} />
		</PageContainer>,
		document.getElementById('root')
	);
})();
