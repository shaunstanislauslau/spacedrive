import React from 'react';
import { ReactComponent as Content } from '~/docs/product/roadmap.md';

import { ReactComponent as Folder } from '../../../../packages/interface/src/assets/svg/folder.svg';
import Markdown from '../components/Markdown';

export function Page() {
	return (
		<Markdown>
			<div className="w-24 mb-10">
				<Folder className="" />
			</div>
			<Content />
		</Markdown>
	);
}

export function onBeforeRender() {
	return {
		pageContext: {
			documentProps: {
				title: 'Roadmap - Spacedrive',
				description: 'What can Spacedrive do?'
			}
		}
	};
}
