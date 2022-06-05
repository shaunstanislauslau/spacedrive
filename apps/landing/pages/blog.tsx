import React from 'react';

export function Page() {
	return <></>;
}

export function onBeforeRender() {
	return {
		pageContext: {
			documentProps: {
				title: 'Changelog - Spacedrive',
				description: 'Updates and release builds of the Spacedrive app.'
			}
		}
	};
}
