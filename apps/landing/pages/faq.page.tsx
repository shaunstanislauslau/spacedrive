import React from 'react';
import { ReactComponent as Content } from '~/docs/product/faq.md';

import Markdown from '../components/Markdown';

export function Page() {
	return (
		<Markdown>
			<Content />
		</Markdown>
	);
}

export function onBeforeRender() {
	return {
		pageContext: {
			documentProps: {
				title: 'FAQ - Spacedrive',
				description: 'Updates and release builds of the Spacedrive app.'
			}
		}
	};
}
