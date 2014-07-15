<%@ Page Title="About" Language="C#" MasterPageFile="~/Site.Master" AutoEventWireup="true" CodeBehind="About.aspx.cs" Inherits="WebApplication1.About" %>
<script src="/Scripts/jquery-1.8.2.js" type="text/javascript"></script>
 
<asp:Content runat="server" ID="BodyContent" ContentPlaceHolderID="MainContent">
    <hgroup class="title">
        <h1><%: Title %>.</h1>
        <h2>Your app description page.</h2>
    </hgroup>

    <article>
        <p>        
            Use this area to provide additional information.
        </p>

        <p>        
            Use this area to provide additional information.
        </p>

        <p>        
            Use this area to provide additional information.
        </p>
    </article>

    <aside>
        <h3>Aside Title</h3>
        <p>        
            Use this area to provide additional information.
        </p>
        <ul>
            <li><a runat="server" href="~/">Home</a></li>
            <li><a runat="server" href="~/About">About</a><a href="Scripts/jquery-1.8.2.js">jquery-1.8.2.js</a></li>
            <li>
                <asp:CheckBoxList ID="CheckBoxList1" runat="server">
                    <asp:ListItem>22</asp:ListItem>
                    <asp:ListItem>33</asp:ListItem>
                    <asp:ListItem>44</asp:ListItem>
                </asp:CheckBoxList>
                
                <script>
                    $(function() {
                        $("#MainContent_CheckBoxList1 > input[type=checkbox]").click(function () {
                            alert(1);
                        });

                    })

                </script>
                <a runat="server" href="~/Contact">Contact</a></li>
        </ul>
    </aside>
</asp:Content>