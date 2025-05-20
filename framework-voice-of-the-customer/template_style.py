sit_style = """
	<style>

	div[data-testid="stApp"]{
	    background-color: #24323D;
	    }

	header[data-testid="stHeader"]{
		display: inline;
		color: white;
	    height:150px;
	    border: 1px solid #ffffff;
	    border-radius: 32px 32px 0px 0px;
	    background-color: #29B5E8;
	    background-size: cover;
	    background-repeat: no-repeat;
	    background-position: center center;
	    text-align: center;
	    margin-left:2rem;
	    margin-top: 2rem;
	    margin-right:2rem;
	    padding-top: 1.25rem;
	    font-size: 60px;
	    z-index: 3;
	    }

	header[data-testid="stHeader"]:after{
		content: "Voice of the Customer";
	}

	[data-testid="stSidebar"] {
		color: white;
	    background-color: #11567f;
	    max-width: 244px;
	    z-index: 4;
	    border-right: 1px solid #ffffff;
	    #border: 1px solid #ffffff;
	    }
	[data-testid="stSidebarCollapsedControl"]{
	    visibility:hidden;
	    }
	div[data-testid="stDecoration"]{
	    visibility:hidden;
	    }

	div[data-testid="stAppViewContainer"]{
	    background-color: #ffffff;
	    padding-top: 5rem;
	    margin-right:2rem;
	    margin-top:2rem;
	    margin-left:2rem;
	    margin-bottom:2rem;
	    border-radius:2rem;
	    border: 1px solid #ffffff;
	    box-shadow: 4px 4px 4px 1px rgba(64, 64, 64, .25);
	    z-index: 1;
	    }

	/* Primary button style, include "primary-button" in key to trigger */
	[class *= "primary-button"] button{
			color: #FFFFFF;
			background-color: #11567F;
		}

	[class *= "primary-button"]:hover button{
			color: #FFFFFF;
			background-color: #044064;
			border-color: transparent;
		}

	[class *= "primary-button"]:active button{
			color: #FFFFFF;
			background-color: #1B78AE;
		}
	[class *= "primary-button"] :focus:not(:active) {
			color: #FFFFFF;
			background-color: #1B78AE;
			border-color: transparent;
		}


	/* Primary button style, include "primary-button" in key to trigger */
	[class *= "secondary-button"] button{
			color: #11567F;
			border-color: #11567F;
		}

	[class *= "secondary-button"]:hover button{
			color: #02263B;
			border-color: #02263B;
		}

	[class *= "secondary-button"]:active button{
			color: #1B78AE;
			border-color: #1B78AE;
			background-color:transparent;
		}
	[class *= "secondary-button"] :focus:not(:active) {
			color: #1B78AE;
			border-color: #1B78AE;
			background-color:transparent;
		}

	[class *= "tertiary-button"] button{
			color: #262730;
			border-color: #BFC5D3;
		}

	[class *= "tertiary-button"]:hover button{
			color: #262730;
			border-color: #808495;
		}

	[class *= "tertiary-button"]:active button{
			color: #262730;
			border-color: #262730;
			background-color:transparent;
		}
	[class *= "tertiary-button"] :focus:not(:active) {
			color: #262730;
			border-color: #262730;
			background-color:transparent;
		}

	[class *= "destructive-button"] button{
			color: #FF2B2B;
			border-color: #FF2B2B;
		}

	[class *= "destructive-button"]:hover button{
			color: #7D353B;
			border-color: #7D353B;
		}

	[class *= "destructive-button"]:active button{
			color: #FF8C8C;
			border-color: #FF8C8C;
			background-color:transparent;
		}
	[class *= "destructive-button"] :focus:not(:active) {
			color: #FF8C8C;
			border-color: #FF8C8C;
			background-color:transparent;
		}




	/* This is the main container for tabs, make sure to include "styled-tabs" in the container surrounding the tabs*/
	[class *= "styled-tabs"] div[data-baseweb="tab-list"]{
	    background:transparent;
	}
	/*Every tab is a button element*/
	[class *= "styled_tabs"] button{
	    # width:33%;
	    # border-radius:10px;
	}

	/*Styles the selected tab*/
	[class *= "styled-tabs"] button[aria-selected="true"]{
	    # background-color:#eaeaea;
	}
	[class *= "styled-tabs"] button[aria-selected="true"] p{
	    color:#11567F !important;
	    # font-weight:bold;
	}

	/* This is the bottom ruler for tabs*/
	[class *= "styled-tabs"] div[data-baseweb="tab-border"]{
	    # background-color:blue;
	}

	/* This highlights selected tab*/
	[class *= "styled-tabs"] div[data-baseweb="tab-highlight"]{
	    background-color:darkgrey;
	    # height:7px;
	}

	/* This is the bottom ruler for tabs*/
	[class *= "styled-tabs"] div[data-baseweb="tab-border"]{
	    # background-color:blue;
	}

	/* This is the body of the tab content*/
	[class *= "styled-tabs"] div[data-baseweb="tab-panel"]{
	    # background-color:#eaeaea;
	}




	[class *= "styled-mselect"] div[data-baseweb="select"] div{
		color: black;
		# background-color: #11567F !important;
		# border-color: #11567F !important;
	}

	[class *= "styled-mselect"] div[data-baseweb="select"] div:hover{
		color: black;
		background-color: #EAEBEC !important;
	}


	[class *= "styled-mselect"] div[data-baseweb="select"] div:active{
		color: black;
		border-color: #11567F !important;
	}

	[class *= "styled-mselect"] div[data-baseweb="select"] div:active{
		color: black;
		border-color: #11567F !important;
	}

	[class *= "styled-mselect"] :has(input[aria-expanded="true"]) div[data-baseweb="select"] div{
		color: black;
		border-color: #11567F !important;
	}

	[class *= "styled-mselect"] span[role="button"]{
		color: white;
		background-color: #11567F !important;
	}



	[class *= "styled-sbox"] div[data-baseweb="select"] div{
		color: black;
	}

	[class *= "styled-sbox"] div[data-baseweb="select"] div:hover{
		color: black;
		background-color: #EAEBEC !important;
	}

	[class *= "styled-sbox"] div[data-baseweb="select"] div:active {
		color: black;
		border-color: #11567F !important;
	}

	[class *= "styled-sbox"] :has(input[aria-expanded="true"]) div[data-baseweb="select"] div{
		color: black;
		border-color: #11567F !important;
	}



	.stCheckbox label:has(input[aria-checked="true"]) > span:first-child {
		background-color: #11567F !important;
		border-color: #11567F !important;
	}



	[class *= "styled-dateinput"] div[data-baseweb="base-input"]:hover{
		background-color: #EAEBEC !important;
	}

	[class *= "styled-dateinput"] div[data-baseweb="input"]:hover{
		background-color: #EAEBEC !important;
	}

	[class *= "styled-dateinput"] div[data-baseweb="input"]:active {
		border-color: #11567F !important;
	}

	[class *= "styled-dateinput"] .focused {
		border-color: #11567F !important;
	}

	div[aria-label*="Calendar."] div[aria-label*="Selected."]::after {
		background-color: #11567F !important;
		border-color: #11567F !important;
	}

	div[aria-label*="Calendar."] div[role="gridcell"]:hover::after {
		border-color: #11567F !important;
	}


	[class *= "styled-numinput"] div[data-baseweb="select"] div{
		color: black;
	}

	[class *= "styled-numinput"] div[data-baseweb="select"]:hover {
		color: black;
		background-color: #EAEBEC !important;
	}

	[class *= "styled-numinput"] div[data-testid="stNumberInputContainer"] > div > div :hover {
		background-color: #EAEBEC !important;
	}

	[class *= "styled-numinput"] div[data-testid="stNumberInputContainer"]:active {
		border-color: #11567F !important;
	}

	[class *= "styled-numinput"] .focused {
		border-color: #11567F !important;
	}

	[class *= "styled-numinput"] button[data-testid="stNumberInputStepDown"]:hover {
		background-color: #11567F !important;
	}
	[class *= "styled-numinput"] button[data-testid="stNumberInputStepUp"]:hover {
		background-color: #11567F !important;
	}

	[class *= "styled-numinput"] button[data-testid="stNumberInputStepDown"]:focus:not(:active) {
		background-color: #11567F !important;
	}
	[class *= "styled-numinput"] button[data-testid="stNumberInputStepUp"]:focus:not(:active) {
		background-color: #11567F !important;
	}


	[class *= "styled-pills"] button:hover {
		color: #11567F;
		border-color: #11567F;
	}
	[class *= "styled-pills"] button[kind*="Active"]{
		background-color: #D4EFFF;
		color: #11567F;
		border-color: #11567F;
	}

	[class *= "styled-segcontrols"] button:hover {
		color: #11567F;
		border-color: #11567F;
	}
	[class *= "styled-segcontrols"] button[kind*="Active"]{
		background-color: #D4EFFF;
		color: #11567F;
		border-color: #11567F;
	}
		[class *= "styled-segcontrols-astabs"] div[data-baseweb="button-group"]{
		max-width: 100% !important;
		flex-wrap: nowrap;
	}
	[class *= "styled-segcontrols-astabs"] div[data-baseweb="button-group"] button{
		flex: 100%;
	}

	</style>"""
