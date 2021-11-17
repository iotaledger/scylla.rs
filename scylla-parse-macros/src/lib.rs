// Copyright 2021 IOTA Stiftung
// SPDX-License-Identifier: Apache-2.0

use proc_macro::TokenStream;
use quote::quote;

#[proc_macro_derive(ParseFromStr)]
pub fn parse_from_str_derive(input: TokenStream) -> TokenStream {
    let syn::DeriveInput {
        attrs: _,
        vis: _,
        ident,
        generics,
        data: _,
    } = syn::parse_macro_input!(input as syn::DeriveInput);
    let (imp, ty, wher) = generics.split_for_impl();
    quote! {
        impl #imp FromStr for #ident #ty #wher {
            type Err = anyhow::Error;
            fn from_str(s: &str) -> anyhow::Result<Self> {
                StatementStream::new(s).parse()
            }
        }
    }
    .into()
}
