import React from 'react';
import { ReactComponent as Content } from '~/docs/product/credits.md';

import Markdown from '../components/Markdown';

export function Page() {
	return (
		<Markdown>
			<div className="team-page">
				<Content />
			</div>
		</Markdown>
	);
}

export function onBeforeRender() {
	return {
		pageContext: {
			documentProps: {
				title: 'Our Team - Spacedrive',
				description: "Who's behind Spacedrive?"
			}
		}
	};
}
