use proc_macro::TokenStream;
use quote::ToTokens;

#[proc_macro_attribute]
pub fn trace(_metadata: TokenStream, input: TokenStream) -> TokenStream {
    let mut f: syn::ItemFn = syn::parse(input).unwrap();
    let name = f.sig.ident.to_string();
    let enter: syn::Stmt = syn::parse_quote! {
        let _span = log::enter(#name);
    };
    f.block.stmts.insert(0, enter);
    f.to_token_stream().into()
}
