import { Button } from '@sd/ui';
import React from 'react';

import { Footer } from '../components/Footer';
import NavBar from '../components/NavBar';
import type { PageContext } from './types';
import { PageContextProvider } from './usePageContext';

export function PageContainer({
	children,
	pageContext
}: {
	children: React.ReactNode;
	pageContext: PageContext;
}) {
	return (
		<React.StrictMode>
			<PageContextProvider pageContext={pageContext}>
				<div className="dark:bg-black dark:text-white ">
					<Button
						href="#content"
						className="fixed left-0 z-50 mt-3 ml-8 duration-200 -translate-y-16 cursor-pointer focus:translate-y-0"
						variant="gray"
					>
						Skip to content
					</Button>

					{/* <NavBar /> */}
					<div className="container z-10 flex flex-col items-center px-4 mx-auto overflow-x-hidden sm:overflow-x-visible ">
						{children}
						<Footer />
					</div>
				</div>
			</PageContextProvider>
		</React.StrictMode>
	);
}
