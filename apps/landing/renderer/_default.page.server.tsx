import React from 'react';
import ReactDOMServer from 'react-dom/server';
import { dangerouslySkipEscape, escapeInject } from 'vite-plugin-ssr';
import type { PageContextBuiltIn } from 'vite-plugin-ssr';

import '@sd/ui/style';

import '../style.scss';
import { PageContainer } from './PageContainer';
import type { PageContext } from './types';

export const passToClient = ['pageProps', 'urlPathname'];

export async function render(pageContext: PageContextBuiltIn & PageContext) {
	const { Page, pageProps, documentProps } = pageContext;

	const pageHtml = ReactDOMServer.renderToString(
		<PageContainer pageContext={pageContext}>
			<Page {...pageProps} />
		</PageContainer>
	);

	const title =
		(documentProps && documentProps.title) || 'Spacedrive — A file manager from the future';
	const desc =
		(documentProps && documentProps.description) ||
		'Combine your drives and clouds into one database that you can organize and explore from any device. Designed for creators, hoarders and the painfully disorganized.';

	const documentHtml = escapeInject`<!DOCTYPE html>
    <html lang="en">
      <head>
        <meta charset="UTF-8" />
        <meta name="viewport" content="width=device-width, initial-scale=1.0" />
        <meta name="description" content="${desc}" />
        <title>${title}</title>
        <meta
            name="og:image"
            content="https://raw.githubusercontent.com/spacedriveapp/.github/main/profile/spacedrive_icon.png"
        />
        <meta
			name="keywords"
			content="files,file manager,spacedrive,file explorer,vdfs,distributed filesystem,cas,content addressable storage,virtual filesystem,photos app, video organizer,video encoder,tags,tag based filesystem"
		/>
      </head>
      <body>
        <div id="root">${dangerouslySkipEscape(pageHtml)}</div>
      </body>
    </html>`;

	return {
		documentHtml
	};
}
