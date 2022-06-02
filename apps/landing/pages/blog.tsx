import React from 'react';
import { Helmet } from 'react-helmet';
import { ReactComponent as Content } from '~/docs/product/roadmap.md';

import { ReactComponent as Folder } from '../../../../packages/interface/src/assets/svg/folder.svg';
import Markdown from '../components/Markdown';

function Page() {
	return (
		<>
			<Helmet>
				<title>Blog &bull; Spacedrive</title>
			</Helmet>
		</>
	);
}

export { Page };
