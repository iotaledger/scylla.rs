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

fn is_wrappable(ty: &syn::Type) -> bool {
    match ty {
        syn::Type::Path(syn::TypePath { qself: None, path }) => {
            if let Some(seg) = path.segments.last() {
                match seg.ident.to_string().as_str() {
                    "Vec" | "Option" | "HashMap" | "Box" | "BTreeMap" | "BTreeSet" | "String" => return true,
                    _ => (),
                }
            }
        }
        _ => (),
    }
    false
}

#[proc_macro_derive(ToTokens, attributes(wrap))]
pub fn to_tokens_derive(input: TokenStream) -> TokenStream {
    let syn::DeriveInput {
        attrs: _,
        vis: _,
        ident,
        generics,
        data,
    } = syn::parse_macro_input!(input as syn::DeriveInput);
    let (imp, ty, wher) = generics.split_for_impl();
    match data {
        syn::Data::Struct(s) => {
            let (destr, restr) = match s.fields {
                syn::Fields::Named(f) => {
                    let names = f.named.iter().map(|f| f.ident.as_ref().unwrap()).collect::<Vec<_>>();
                    let assigns = names.iter().map(|n| quote!(#n: ##n));
                    let wrapped = f
                        .named
                        .iter()
                        .map(|f| f.attrs.iter().find(|a| a.path.is_ident("wrap")).is_some() || is_wrappable(&f.ty))
                        .zip(names.iter())
                        .map(|(w, n)| {
                            if w {
                                quote!(TokenWrapper(&self.#n))
                            } else {
                                quote!(&self.#n)
                            }
                        });
                    (
                        quote! {
                            let (#(#names),*) = (#(#wrapped),*);
                        },
                        quote! {
                            #ident { #(#assigns),* }
                        },
                    )
                }
                syn::Fields::Unnamed(f) => {
                    let names = f
                        .unnamed
                        .iter()
                        .enumerate()
                        .map(|(i, _)| {
                            let idx = syn::Index::from(i);
                            quote::format_ident!("f_{}", idx)
                        })
                        .collect::<Vec<_>>();
                    let assigns = names.iter().map(|n| quote!(##n));
                    let wrapped = f
                        .unnamed
                        .iter()
                        .map(|f| f.attrs.iter().find(|a| a.path.is_ident("wrap")).is_some() || is_wrappable(&f.ty))
                        .enumerate()
                        .map(|(i, w)| {
                            let idx = syn::Index::from(i);
                            if w {
                                quote!(TokenWrapper(&self.#idx))
                            } else {
                                quote!(&self.#idx)
                            }
                        });
                    (
                        quote! {
                            let (#(#names),*) = (#(#wrapped),*);
                        },
                        quote! {
                            #ident ( #(#assigns),* )
                        },
                    )
                }
                syn::Fields::Unit => (quote!(), quote!( #ident )),
            };
            quote! {
                impl<'a> #imp CustomToTokens<'a> for #ident #ty #wher {
                    fn to_tokens(&'a self, tokens: &mut quote::__private::TokenStream) {
                        #destr
                        tokens.extend(quote::quote!(#restr));
                    }
                }

                impl #imp quote::ToTokens for #ident #ty #wher {
                    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
                        CustomToTokens::to_tokens(self, tokens);
                    }
                }
            }
        }
        syn::Data::Enum(e) => {
            let variants = e.variants.into_iter().map(|v| {
                let var_id = &v.ident;
                let (destr, restr) = match v.fields {
                    syn::Fields::Named(f) => {
                        let names = f.named.iter().map(|f| f.ident.as_ref().unwrap()).collect::<Vec<_>>();
                        let assigns = names.iter().map(|n| quote!(#n: ##n));
                        let wrapped = f
                            .named
                            .iter()
                            .map(|f| f.attrs.iter().find(|a| a.path.is_ident("wrap")).is_some() || is_wrappable(&f.ty))
                            .zip(names.iter())
                            .map(|(w, n)| if w { quote!(TokenWrapper(#n)) } else { quote!(#n) });
                        (
                            quote! {
                                { #(#names),* }
                            },
                            quote! {
                                {
                                    let (#(#names),*) = (#(#wrapped),*);
                                    quote::quote!(#ident::#var_id { #(#assigns),* })
                                }
                            },
                        )
                    }
                    syn::Fields::Unnamed(f) => {
                        let names = f
                            .unnamed
                            .iter()
                            .enumerate()
                            .map(|(i, _)| {
                                let idx = syn::Index::from(i);
                                quote::format_ident!("f_{}", idx)
                            })
                            .collect::<Vec<_>>();
                        let assigns = names.iter().map(|n| quote!(##n));
                        let wrapped = f
                            .unnamed
                            .iter()
                            .map(|f| f.attrs.iter().find(|a| a.path.is_ident("wrap")).is_some() || is_wrappable(&f.ty))
                            .zip(names.iter())
                            .map(|(w, n)| if w { quote!(TokenWrapper(#n)) } else { quote!(#n) });
                        (
                            quote! {
                                ( #(#names),* )
                            },
                            quote! {
                                {
                                    let (#(#names),*) = (#(#wrapped),*);
                                    quote::quote!(#ident::#var_id ( #(#assigns),* ))
                                }
                            },
                        )
                    }
                    syn::Fields::Unit => (quote!(), quote!(quote::quote!(#ident::#var_id))),
                };
                quote! {
                    #ident::#var_id #destr => #restr
                }
            });
            quote! {
                impl<'a> #imp CustomToTokens<'a> for #ident #ty #wher {
                    fn to_tokens(&'a self, tokens: &mut quote::__private::TokenStream) {
                        tokens.extend(match self {
                            #(#variants),*
                        })
                    }
                }

                impl #imp quote::ToTokens for #ident #ty #wher {
                    fn to_tokens(&self, tokens: &mut quote::__private::TokenStream) {
                        CustomToTokens::to_tokens(self, tokens);
                    }
                }
            }
        }
        syn::Data::Union(_) => panic!("Unions not supported!"),
    }
    .into()
}
